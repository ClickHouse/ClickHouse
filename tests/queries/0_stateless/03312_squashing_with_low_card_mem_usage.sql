-- Tags: no-tsan, no-asan, no-msan, no-ubsan, no-random-settings, no-random-merge-tree-settings
-- no sanitizers -- memory consumption is unpredicatable with sanitizers
-- no random settings -- it was quite hard to reproduce and I'm afraid that settings randomisation will make the test weaker

drop table if exists t;
create table t(s LowCardinality(String)) Engine = MergeTree order by tuple() settings min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

-- The problem was that we didn't account for dictionary size in `ColumnLowCardinality::byteSize()`.
-- Because of that we tend to accumulate too many blocks in `SimpleSquashingChunksTransform`.
-- To reproduce we need a column with heavy dictionaries and ideally nothing else consuming significant amount of memory.
insert into t select repeat('x', 1000) || toString(number) as s from numbers_mt(5e6) settings max_insert_threads = 16, max_memory_usage = '15Gi';

WITH t2 AS
    (
        SELECT
            'x' AS s,
            number
        FROM numbers_mt(10000.)
    )
SELECT t1.s
FROM t AS t1
INNER JOIN t2 ON substr(t1.s, 1, 1) = t2.s
LIMIT 1e5
SETTINGS max_threads = 32, max_memory_usage = '2Gi', join_algorithm = 'parallel_hash', min_joined_block_size_bytes = '1Mi'
FORMAT Null;
