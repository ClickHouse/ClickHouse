-- Tags: no-random-merge-tree-settings

SET optimize_trivial_insert_select = 1;

drop table if exists test_02381;
create table test_02381(a UInt64, b UInt64) ENGINE = MergeTree order by (a, b) SETTINGS compress_marks = false, compress_primary_key = false, ratio_of_defaults_for_sparse_serialization = 1;
insert into test_02381 select number, number * 10 from system.numbers limit 1000000;

drop table if exists test_02381_compress;
create table test_02381_compress(a UInt64, b UInt64) ENGINE = MergeTree order by (a, b)
    SETTINGS compress_marks = true, compress_primary_key = true, marks_compression_codec = 'ZSTD(3)', primary_key_compression_codec = 'ZSTD(3)', marks_compress_block_size = 65536, primary_key_compress_block_size = 65536, ratio_of_defaults_for_sparse_serialization = 1;
insert into test_02381_compress select number, number * 10 from system.numbers limit 1000000;

select * from test_02381_compress where a = 1000 limit 1;
optimize table test_02381_compress final;
select * from test_02381_compress where a = 1000 limit 1;

-- Compare the size of marks on disk
select table, sum(rows), sum(bytes_on_disk) sum_bytes, sum(marks_bytes) sum_marks_bytes, (sum_bytes - sum_marks_bytes) exclude_marks from system.parts_columns where active and database = currentDatabase() and table like 'test_02381%' group by table order by table;

-- Switch to compressed and uncompressed
-- Test wide part
alter table test_02381 modify setting compress_marks=true, compress_primary_key=true;
insert into test_02381 select number, number * 10 from system.numbers limit 1000000;

alter table test_02381_compress modify setting compress_marks=false, compress_primary_key=false;
insert into test_02381_compress select number, number * 10 from system.numbers limit 1000000;

select * from test_02381_compress where a = 10000 limit 1;
optimize table test_02381_compress final;
select * from test_02381_compress where a = 10000 limit 1;

select * from test_02381 where a = 10000 limit 1;
optimize table test_02381 final;
select * from test_02381 where a = 10000 limit 1;

select table, sum(rows), sum(bytes_on_disk) sum_bytes, sum(marks_bytes) sum_marks_bytes, (sum_bytes - sum_marks_bytes) exclude_marks  from system.parts_columns where active and  database = currentDatabase() and table like 'test_02381%' group by table order by table;

drop table if exists test_02381;
drop table if exists test_02381_compress;

-- Test compact part
drop table if exists test_02381_compact;
create table test_02381_compact (a UInt64, b String) ENGINE = MergeTree order by (a, b);

insert into test_02381_compact values (1, 'Hello');
alter table test_02381_compact modify setting compress_marks = true, compress_primary_key = true;
insert into test_02381_compact values (2, 'World');

select * from test_02381_compact order by a;
optimize table test_02381_compact final;
select * from test_02381_compact order by a;

drop table if exists test_02381_compact;
