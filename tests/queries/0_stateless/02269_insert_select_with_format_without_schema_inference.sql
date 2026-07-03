insert into function file(currentDatabase() || '_02269_data', 'RowBinary') select 1 settings engine_file_truncate_on_insert=1;
select * from file(currentDatabase() || '_02269_data', 'RowBinary', 'x UInt8');
