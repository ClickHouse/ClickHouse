drop table if exists test_02381;
create table test_02381(a UInt64, b UInt64) ENGINE = MergeTree order by (a, b);
insert into test_02381 select number, number from system.numbers limit 1000000;

drop table if exists test_02381_compress;
create table test_02381_compress(a UInt64, b UInt64) ENGINE = MergeTree order by (a, b)
    SETTINGS compress_marks=1, compress_primary_key=1, marks_compression_codec='ZSTD(3)', primary_key_compression_codec='ZSTD(3)', marks_compress_block_size=65536, primary_key_compress_block_size=65536;
insert into test_02381_compress select number, number * 10 from system.numbers limit 1000000;

-- Compare the size of marks on disk
select table,sum(marks_bytes) from system.parts_columns where database = currentDatabase() and table like 'test_02381%' group by table order by table;

select count(*) from test_02381_compress;
select * from test_02381_compress where a = 1000;

drop table if exists test_02381;
drop table if exists test_02381_compress;
