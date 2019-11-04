
IF OBJECT_ID ('[dbo].[AutoShrink]', 'P' ) IS NOT NULL
    DROP PROCEDURE [dbo].[AutoShrink];
GO

CREATE PROCEDURE [dbo].[AutoShrink]

AS

truncate table DBA.dbo.shrinkdatafiles;

CREATE TABLE #TEMP_TABLE_OLD_VALUES (
	databasename varchar(128),
	databaseid int,
	databaselogname varchar(128),
	oldsize int
)

/*
Mail ile aşağidaki sorgularin sonuclari gonderildi.
select servername, sum(oldsize) - sum(newsize) 'difference', groupname from DBA.dbo.shrinkdatafiles group by servername, groupname
select servername, status, groupname from  DBA.dbo.shrinkdatafiles where status != 'OK';
select groupname, servername, databasename, logfilename, oldsize, newsize from DBA.dbo.shrinkdatafiles where newsize > 51200 and oldsize - newsize < 1024;



Mail yollamak icin SMTP Ayari yapildi. 
Management-> Database Mail -> Manage Database Mail Account and profiles -> view change or delete and existing account -> 
	servername = 192.168.134.238  port :25 yapilip kaydedilir. Bu sekilde mail yollar

*/

DECLARE @SERVER_NAME VARCHAR(128);
DECLARE @SQL_NAME VARCHAR(128);
DECLARE @LINKEDSERVER_NAME VARCHAR(128);
DECLARE @DB_VERSION VARCHAR(50);
DECLARE @DATABASE_NAME VARCHAR(128)
DECLARE @DATABASE_ID INT
DECLARE @DATABASE_LOG_NAME VARCHAR(256)
DECLARE @OLD_LOG_SIZE FLOAT
DECLARE @TABLE_ROWS VARCHAR(MAX) = ''
DECLARE @ERROR_TABLE_ROWS VARCHAR(MAX) = ''
DECLARE @BIG_LOGS_TABLE_ROWS VARCHAR(MAX) = ''
DECLARE @DIFFERENCE_SIZE INT
DECLARE @TOTAT_DIFFERENCE INT = 0
DECLARE @ERROR_STATUS VARCHAR(256)


IF CURSOR_STATUS('global','SERVER_CURSOR')=1
	CLOSE SERVER_CURSOR;

IF CURSOR_STATUS('global','SERVER_CURSOR')=-1
	DEALLOCATE SERVER_CURSOR;


DECLARE SERVER_CURSOR CURSOR FOR 
SELECT groups.name, svr.name FROM msdb.dbo.sysmanagement_shared_server_groups_internal groups 
INNER JOIN msdb.dbo.sysmanagement_shared_registered_servers_internal svr
ON groups.server_group_id = svr.server_group_id;

OPEN SERVER_CURSOR
FETCH NEXT FROM SERVER_CURSOR INTO @SQL_NAME, @SERVER_NAME;

WHILE @@FETCH_STATUS =0
BEGIN
	SET @LINKEDSERVER_NAME = 'AUTOSHRINK_' + @SERVER_NAME

	BEGIN
		
		BEGIN TRY
			EXEC sp_addlinkedserver @server = @LINKEDSERVER_NAME, @srvproduct=N'',  @provider=N'SQLNCLI',  @datasrc = @SERVER_NAME;
			EXEC sp_serveroption @server = @LINKEDSERVER_NAME, @optname = 'rpc out', @optvalue = 'True';
			EXEC sp_addlinkedsrvlogin @LINKEDSERVER_NAME, 'FALSE', NULL, 'username_here', 'password_here';
		END TRY
		BEGIN CATCH
			IF ERROR_MESSAGE() NOT LIKE '%already exists%'
				BEGIN
					insert into DBA.dbo.shrinkdatafiles values(SYSDATETIME(), @SQL_NAME, NULL, @SERVER_NAME, NULL, NULL, 0, 0, 'ERROR: ' + ERROR_MESSAGE());
					FETCH NEXT FROM SERVER_CURSOR INTO @SQL_NAME, @SERVER_NAME
					CONTINUE
				END
				
		END CATCH

	END
	BEGIN

		
		BEGIN TRY			
			IF CURSOR_STATUS('global','DATABASES_VERSION_CURSOR')=1
				CLOSE DATABASES_VERSION_CURSOR

			IF CURSOR_STATUS('global','DATABASES_VERSION_CURSOR')=-1
				DEALLOCATE DATABASES_VERSION_CURSOR

			DECLARE @VERSION_QUERY NVARCHAR(256) =  N'DECLARE DATABASES_VERSION_CURSOR CURSOR FOR SELECT * FROM OPENQUERY([' + @LINKEDSERVER_NAME + '], ''SELECT @@VERSION'' )'
			EXEC  sp_executesql @VERSION_QUERY
			OPEN DATABASES_VERSION_CURSOR
			FETCH NEXT FROM DATABASES_VERSION_CURSOR INTO @DB_VERSION
			CLOSE DATABASES_VERSION_CURSOR
			DEALLOCATE DATABASES_VERSION_CURSOR


			IF @DB_VERSION LIKE '%Microsoft SQL Server 2008%'
				SET @DB_VERSION = 'SQL2008'
			ELSE IF @DB_VERSION LIKE '%Microsoft SQL Server 2012%'
				SET @DB_VERSION = 'SQL2012'
			ELSE IF @DB_VERSION LIKE '%Microsoft SQL Server 2014%'
				SET @DB_VERSION = 'SQL2014'
			ELSE IF @DB_VERSION LIKE '%Microsoft SQL Server 2016%'
				SET @DB_VERSION = 'SQL2016'


			IF CURSOR_STATUS('global','DATABASES_CURSOR')=1
				CLOSE DATABASES_CURSOR

			IF CURSOR_STATUS('global','DATABASES_CURSOR')=-1
				DEALLOCATE DATABASES_CURSOR

			DECLARE @OLD_DATA_QUERY NVARCHAR(MAX) =  N'DECLARE DATABASES_CURSOR CURSOR FOR 
				select datas.name, datas.database_id, logs.name, logs.size * 8.0 / 1024 from [' + @LINKEDSERVER_NAME + '].[master].[sys].databases datas 
				join [' + @LINKEDSERVER_NAME + '].[master].[sys].master_files logs 
				on datas.database_id = logs.database_id 
				where logs.type_desc = ''LOG'' and datas.database_id > 4'

			EXEC  sp_executesql @OLD_DATA_QUERY

			OPEN DATABASES_CURSOR

			truncate table #TEMP_TABLE_OLD_VALUES;
		
			FETCH NEXT FROM DATABASES_CURSOR INTO @DATABASE_NAME, @DATABASE_ID, @DATABASE_LOG_NAME, @OLD_LOG_SIZE
			WHILE @@FETCH_STATUS = 0
			BEGIN
				DECLARE @SHRINK_QUERY NVARCHAR(MAX) = N'exec (''USE [' + @DATABASE_NAME + '] DBCC SHRINKFILE (N''''' + @DATABASE_LOG_NAME + ''''' , 0, TRUNCATEONLY) WITH NO_INFOMSGS'') at [' + @LINKEDSERVER_NAME  + ']'

				EXEC sp_executesql @SHRINK_QUERY;
				
				insert into #TEMP_TABLE_OLD_VALUES values(@DATABASE_NAME, @DATABASE_ID, @DATABASE_LOG_NAME, @OLD_LOG_SIZE)
				
				FETCH NEXT FROM DATABASES_CURSOR INTO @DATABASE_NAME, @DATABASE_ID, @DATABASE_LOG_NAME, @OLD_LOG_SIZE
			END
				
			DECLARE @NEW_LOG_SIZE FLOAT
			
								
			IF CURSOR_STATUS('global','DATABASES_CURSOR_SHRINK')=1
				CLOSE DATABASES_CURSOR_SHRINK

			IF CURSOR_STATUS('global','DATABASES_CURSOR_SHRINK')=-1
				DEALLOCATE DATABASES_CURSOR_SHRINK


			DECLARE @NEW_DATA_QUERY NVARCHAR(MAX) = N'DECLARE DATABASES_CURSOR_SHRINK CURSOR FOR 
				select datas.databasename, datas.databaselogname, datas.oldsize, logs.size * 8.0 / 1024 
				from #TEMP_TABLE_OLD_VALUES datas 
				join [' + @LINKEDSERVER_NAME + '].[master].[sys].master_files logs 
				on datas.databaseid = logs.database_id and datas.databaselogname COLLATE DATABASE_DEFAULT = logs.name COLLATE DATABASE_DEFAULT 
				where logs.type_desc = ''LOG'' and logs.database_id > 4'
				
			EXEC  sp_executesql @NEW_DATA_QUERY
			
			OPEN DATABASES_CURSOR_SHRINK
			FETCH NEXT FROM DATABASES_CURSOR_SHRINK INTO @DATABASE_NAME, @DATABASE_LOG_NAME, @OLD_LOG_SIZE, @NEW_LOG_SIZE

			WHILE @@FETCH_STATUS = 0
			BEGIN

				insert into DBA.dbo.shrinkdatafiles values(SYSDATETIME(), @SQL_NAME, @DB_VERSION, @SERVER_NAME, @DATABASE_NAME, @DATABASE_LOG_NAME, CAST(@OLD_LOG_SIZE AS INT), CAST(@NEW_LOG_SIZE AS INT), 'OK');

				FETCH NEXT FROM DATABASES_CURSOR_SHRINK INTO @DATABASE_NAME, @DATABASE_LOG_NAME, @OLD_LOG_SIZE, @NEW_LOG_SIZE
			END
			CLOSE DATABASES_CURSOR
			DEALLOCATE DATABASES_CURSOR

			CLOSE DATABASES_CURSOR_SHRINK
			DEALLOCATE DATABASES_CURSOR_SHRINK



		END TRY
		BEGIN CATCH
			
			insert into DBA.dbo.shrinkdatafiles values(SYSDATETIME(), @SQL_NAME, NULL, @SERVER_NAME, NULL, NULL, 0, 0, 'ERROR: ' + ERROR_MESSAGE());

		END CATCH

	END

	BEGIN
		EXEC master.dbo.sp_dropserver @server=@LINKEDSERVER_NAME, @droplogins='droplogins'
	END
       FETCH NEXT FROM SERVER_CURSOR INTO @SQL_NAME, @SERVER_NAME
END


IF CURSOR_STATUS('global','SERVER_CURSOR')=1
	CLOSE SERVER_CURSOR

IF CURSOR_STATUS('global','SERVER_CURSOR')=-1
	DEALLOCATE SERVER_CURSOR



IF CURSOR_STATUS('global','RESULT_CURSOR')=1
	CLOSE RESULT_CURSOR

IF CURSOR_STATUS('global','RESULT_CURSOR')=-1
	DEALLOCATE RESULT_CURSOR


DECLARE RESULT_CURSOR CURSOR FOR select servername, sum(oldsize) - sum(newsize) 'difference', groupname from DBA.dbo.shrinkdatafiles group by servername, groupname;
OPEN RESULT_CURSOR
FETCH NEXT FROM RESULT_CURSOR INTO @SERVER_NAME, @DIFFERENCE_SIZE, @SQL_NAME

WHILE @@FETCH_STATUS = 0
BEGIN
	IF @DIFFERENCE_SIZE < 0
		SET @DIFFERENCE_SIZE = 0
	SET @TOTAT_DIFFERENCE = @TOTAT_DIFFERENCE + @DIFFERENCE_SIZE
	SET @TABLE_ROWS = @TABLE_ROWS + '
	<tr>
	<td>' + @SQL_NAME + '</td>
	<td>' + @SERVER_NAME + '</td>
	<td>' +  CAST(@DIFFERENCE_SIZE AS VARCHAR(64)) + '</td>
	</tr>'
	FETCH NEXT FROM RESULT_CURSOR INTO @SERVER_NAME, @DIFFERENCE_SIZE, @SQL_NAME
END

CLOSE RESULT_CURSOR
DEALLOCATE RESULT_CURSOR

SET @TABLE_ROWS = '
		<b><font size=5>SQL Server Auto Shrink Report</font></b>
		<table border="1">
			<tr>
				<td><b><font size=3>Folder Name</font></b></td>
				<td><b><font size=3>Server Name</font></b></td>
				<td><b><font size=3>Difference (MB)</font></b></td>
			</tr>
			' + @TABLE_ROWS + '
			<tr>
				<td colspan="2"><b><font size=3>Total</font></b></td>
				<td><b><font size=3>' +  CAST(@TOTAT_DIFFERENCE AS VARCHAR(64)) + '</font></b></td>
			</tr>
		</table>
'


IF CURSOR_STATUS('global','RESULT_CURSOR')=1
	CLOSE RESULT_CURSOR

IF CURSOR_STATUS('global','RESULT_CURSOR')=-1
	DEALLOCATE RESULT_CURSOR


DECLARE RESULT_CURSOR CURSOR FOR select servername, status, groupname from  DBA.dbo.shrinkdatafiles where status != 'OK';

OPEN RESULT_CURSOR
FETCH NEXT FROM RESULT_CURSOR INTO @SERVER_NAME, @ERROR_STATUS, @SQL_NAME

WHILE @@FETCH_STATUS = 0
BEGIN
	
	SET @ERROR_TABLE_ROWS = @ERROR_TABLE_ROWS + '
	<tr>
	<td>' + @SQL_NAME + '</td>
	<td>' + @SERVER_NAME + '</td>
	<td>' +  @ERROR_STATUS + '</td>
	</tr>'
	FETCH NEXT FROM RESULT_CURSOR INTO @SERVER_NAME, @ERROR_STATUS, @SQL_NAME
END


IF CURSOR_STATUS('global','RESULT_CURSOR')=1
	CLOSE RESULT_CURSOR

IF CURSOR_STATUS('global','RESULT_CURSOR')=-1
	DEALLOCATE RESULT_CURSOR


SET @ERROR_TABLE_ROWS = '
		<b><font size=5>Auto Shrink Report Error List</font></b>
		<table border="1">
			<tr>
				<td><b><font size=3>Folder Name</font></b></td>
				<td><b><font size=3>Server Name</font></b></td>
				<td><b><font size=3>Status</font></b></td>
			</tr>
			' + @ERROR_TABLE_ROWS + '
		</table>
'





IF CURSOR_STATUS('global','RESULT_CURSOR')=1
	CLOSE RESULT_CURSOR

IF CURSOR_STATUS('global','RESULT_CURSOR')=-1
	DEALLOCATE RESULT_CURSOR


DECLARE RESULT_CURSOR CURSOR FOR select groupname, servername, databasename, logfilename, oldsize, newsize from DBA.dbo.shrinkdatafiles where newsize > 51200 and oldsize - newsize < 1024;


OPEN RESULT_CURSOR
FETCH NEXT FROM RESULT_CURSOR INTO @SQL_NAME, @SERVER_NAME, @DATABASE_NAME, @DATABASE_LOG_NAME, @OLD_LOG_SIZE, @NEW_LOG_SIZE

WHILE @@FETCH_STATUS = 0
BEGIN
	
	SET @BIG_LOGS_TABLE_ROWS = @BIG_LOGS_TABLE_ROWS + '
	<tr>
	<td>' + @SQL_NAME + '</td>
	<td>' + @SERVER_NAME + '</td>
	<td>' +  @DATABASE_NAME + '</td>
	<td>' +  @DATABASE_LOG_NAME + '</td>
	<td>' +  CAST(@OLD_LOG_SIZE AS VARCHAR(20)) + '</td>
	<td>' + CAST(@NEW_LOG_SIZE AS VARCHAR(20)) + '</td>
	</tr>'
	FETCH NEXT FROM RESULT_CURSOR INTO @SQL_NAME, @SERVER_NAME, @DATABASE_NAME, @DATABASE_LOG_NAME, @OLD_LOG_SIZE, @NEW_LOG_SIZE

END


IF CURSOR_STATUS('global','RESULT_CURSOR')=1
	CLOSE RESULT_CURSOR

IF CURSOR_STATUS('global','RESULT_CURSOR')=-1
	DEALLOCATE RESULT_CURSOR


SET @BIG_LOGS_TABLE_ROWS = '
		<b><font size=5>Auto Shrink Big Log Files ( 50G> )</font></b>
		<table border="1">
			<tr>
				<td><b><font size=3>Folder Name</font></b></td>
				<td><b><font size=3>Server Name</font></b></td>
				<td><b><font size=3>Database Name</font></b></td>
				<td><b><font size=3>Log File Name</font></b></td>
				<td><b><font size=3>Old Size (MB)</font></b></td>
				<td><b><font size=3>New Size (MB)</font></b></td>
			</tr>
			' + @BIG_LOGS_TABLE_ROWS + '
		</table>
'



SET @TABLE_ROWS = '
<html>
	<body>
			' + @TABLE_ROWS + '
			<br>
			' + @ERROR_TABLE_ROWS + '
			<br>
			' + @BIG_LOGS_TABLE_ROWS + '
	</body>
</html>'

EXEC msdb.dbo.sp_send_dbmail  
@profile_name = 'Database Mail Profile',  
@recipients = 'mail_here',  
@subject = 'SQL Server Auto Shrink Report',  
@body_format ='HTML',
@body = @TABLE_ROWS;   


GO
