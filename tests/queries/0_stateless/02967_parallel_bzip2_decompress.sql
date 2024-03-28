-- Tags: no-parallel, no-fasttest
set allow_parallel_decompress = true;
set max_download_buffer_size = 1048576;
set engine_file_truncate_on_insert = 1;

-- clickhouse-client should not use parallel decompression, because parallel decompression depends on io thread pool,
-- which is not initialized in clickhouse-client.
SELECT number, number::String FROM numbers(1000000) INTO OUTFILE 'data_02967_1.csv.bz2' TRUNCATE COMPRESSION 'bz2';
CREATE TABLE test_02967 (x UInt64, y String) ENGINE = Memory;
INSERT INTO test_02967 (x, y) FROM INFILE 'data_02967_1.csv.bz2' COMPRESSION 'bz2';
DROP TABLE IF EXISTS test_02967;

insert into function file('data_02967_2.csv.bz2', 'auto', 'x UInt64, y String', 'bz2')
SELECT number, number::String FROM numbers(1000000);

desc file('data_02967_2.csv.bz2');
select count(1) from file('data_02967_2.csv.bz2');
select * from file('data_02967_2.csv.bz2') order by 1 limit 10;
