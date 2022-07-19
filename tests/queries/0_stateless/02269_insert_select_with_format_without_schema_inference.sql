insert into function file('02269_data', 'RowBinary') select 1;
select * from file('02269_data', 'RowBinary', 'x UInt8');
