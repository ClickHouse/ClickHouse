create temporary table 02271_temporary_table_show_rows_bytes (A Int64) Engine=Memory as select * from numbers(1000);

select database, name, total_rows, total_bytes from system.tables where name = '02271_temporary_table_show_rows_bytes';
