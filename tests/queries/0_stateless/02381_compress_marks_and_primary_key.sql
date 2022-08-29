drop table if exists test_02381;
create table test_02381(a UInt64, b UInt64) ENGINE = MergeTree order by (a, b);
insert into test_02381 select number, number * 10 from system.numbers limit 1000000;

drop table if exists test_02381_compress;
create table test_02381_compress(a UInt64, b UInt64) ENGINE = MergeTree order by (a, b)
    SETTINGS compress_marks=true, compress_primary_key=true, marks_compression_codec='ZSTD(3)', primary_key_compression_codec='ZSTD(3)', marks_compress_block_size=65536, primary_key_compress_block_size=65536;
insert into test_02381_compress select number, number * 10 from system.numbers limit 1000000;

select * from test_02381_compress where a = 1000 limit 1;
OPTIMIZE TABLE test_02381_compress FINAL;
select * from test_02381_compress where a = 1000 limit 1;

-- Compare the size of marks on disk
select table, sum(rows), sum(bytes_on_disk) sum_bytes, sum(marks_bytes) sum_marks_bytes, (sum_bytes - sum_marks_bytes) exclude_marks from system.parts_columns where active and database = currentDatabase() and table like 'test_02381%' group by table order by table;

-- switch to compressed and uncompressed
alter table test_02381 modify setting compress_marks=true, compress_primary_key=true;
insert into test_02381 select number, number * 10 from system.numbers limit 1000000;

alter table test_02381_compress modify setting compress_marks=false, compress_primary_key=false;
insert into test_02381_compress select number, number * 10 from system.numbers limit 1000000;

select * from test_02381_compress where a = 10000 limit 1;
OPTIMIZE TABLE test_02381_compress FINAL;
select * from test_02381_compress where a = 10000 limit 1;

select * from test_02381 where a = 10000 limit 1;
OPTIMIZE TABLE test_02381 FINAL;
select * from test_02381 where a = 10000 limit 1;

select table, sum(rows), sum(bytes_on_disk) sum_bytes, sum(marks_bytes) sum_marks_bytes, (sum_bytes - sum_marks_bytes) exclude_marks  from system.parts_columns where active and  database = currentDatabase() and table like 'test_02381%' group by table order by table;

DROP TABLE IF EXISTS test;
CREATE TABLE test (a UInt64, b String) ENGINE = MergeTree order by (a, b);

INSERT INTO test VALUES (1, 'Hello');
ALTER TABLE test MODIFY SETTING compress_marks = true, compress_primary_key = true;
INSERT INTO test VALUES (2, 'World');

SELECT * FROM test ORDER BY a;
OPTIMIZE TABLE test FINAL;
SELECT * FROM test ORDER BY a;

DROP TABLE test;

drop table if exists test_02381;
drop table if exists test_02381_compress;
