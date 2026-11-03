-- Tags: no-random-merge-tree-settings
-- add_minmax_index_for_numeric_columns=0: Different sizes

SET optimize_trivial_insert_select = 1;

drop table if exists test_02381;
create table test_02381(a UInt64, b UInt64) ENGINE = MergeTree order by (a, b) SETTINGS compress_marks = false, compress_primary_key = false, ratio_of_defaults_for_sparse_serialization = 1, serialization_info_version = 'basic', auto_statistics_types = '', add_minmax_index_for_numeric_columns=0;
insert into test_02381 select number, number * 10 from system.numbers limit 1000000;

drop table if exists test_02381_compress;
create table test_02381_compress(a UInt64, b UInt64) ENGINE = MergeTree order by (a, b)
    SETTINGS compress_marks = true, compress_primary_key = true, marks_compression_codec = 'ZSTD(3)', primary_key_compression_codec = 'ZSTD(3)', marks_compress_block_size = 65536, primary_key_compress_block_size = 65536, ratio_of_defaults_for_sparse_serialization = 1, serialization_info_version = 'basic', auto_statistics_types = '', add_minmax_index_for_numeric_columns=0;
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
create table test_02381_compact (a UInt64, b String) ENGINE = MergeTree order by (a, b) SETTINGS auto_statistics_types = '';

insert into test_02381_compact values (1, 'Hello');
alter table test_02381_compact modify setting compress_marks = true, compress_primary_key = true;
insert into test_02381_compact values (2, 'World');

select * from test_02381_compact order by a;
optimize table test_02381_compact final;
select * from test_02381_compact order by a;

drop table if exists test_02381_compact;

-- Coverage for MergeTreeMarksLoader.cpp streaming path (lines 194-195, 215-227, 249-271, 284-286).
-- use_streaming_marks_compression = 1 exercises the streaming-decompression branch that is never
-- taken by any other CI test. Three table variants: wide-adaptive, wide-constant, compact.

-- 1. Wide format, adaptive granularity — adaptive streaming path (lines 249-271)
CREATE TABLE t_stream_wide_adaptive (a UInt64, b String)
ENGINE = MergeTree ORDER BY a;
INSERT INTO t_stream_wide_adaptive SELECT number, toString(number) FROM numbers(10000);
SELECT count(), sum(a) FROM t_stream_wide_adaptive SETTINGS use_streaming_marks_compression = 1;
SELECT count(), sum(a) FROM t_stream_wide_adaptive;
DROP TABLE t_stream_wide_adaptive;

-- 2. Wide format, constant granularity — non-adaptive streaming path (lines 215-227)
CREATE TABLE t_stream_wide_constant (a UInt64, b String)
ENGINE = MergeTree ORDER BY a
SETTINGS index_granularity_bytes = 0, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO t_stream_wide_constant SELECT number, toString(number) FROM numbers(10000);
SELECT count(), sum(a) FROM t_stream_wide_constant SETTINGS use_streaming_marks_compression = 1;
SELECT count(), sum(a) FROM t_stream_wide_constant;
DROP TABLE t_stream_wide_constant;

-- 3. Compact format — compact marks streaming path (lines 284-286)
CREATE TABLE t_stream_compact (a UInt64, b UInt64, c UInt64, d UInt64, e String)
ENGINE = MergeTree ORDER BY a
SETTINGS min_rows_for_wide_part = 100000, min_bytes_for_wide_part = 10000000;
INSERT INTO t_stream_compact SELECT number, number*2, number*3, number*4, toString(number) FROM numbers(1000);
SELECT part_type FROM system.parts
WHERE database = currentDatabase() AND table = 't_stream_compact' AND active
ORDER BY part_type;
SELECT count(), sum(a), sum(b) FROM t_stream_compact SETTINGS use_streaming_marks_compression = 1;
SELECT count(), sum(a), sum(b) FROM t_stream_compact;
DROP TABLE t_stream_compact;
