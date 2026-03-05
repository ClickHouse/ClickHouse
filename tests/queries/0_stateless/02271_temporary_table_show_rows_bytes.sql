-- NOTE: database = currentDatabase() is not mandatory

CREATE TEMPORARY TABLE 02271_temporary_table_show_rows_bytes (A Int64) Engine=Memory as SELECT * FROM numbers(1000);
SELECT database, name, total_rows, total_bytes FROM system.tables WHERE is_temporary AND name = '02271_temporary_table_show_rows_bytes';
