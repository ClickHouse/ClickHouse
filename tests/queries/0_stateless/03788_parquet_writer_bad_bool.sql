-- Tags: no-fasttest

insert into function file(currentDatabase() || '.parquet') select x from format(RowBinary, 'x Bool', 'a');
select x, reinterpret(x, 'UInt8') from file(currentDatabase() || '.parquet');
