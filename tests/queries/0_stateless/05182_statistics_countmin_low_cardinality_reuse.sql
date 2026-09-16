-- Tags: no-fasttest
-- `countmin` requires the DataSketches library, which is unavailable in fasttest.

SET explain_query_plan_default = 'legacy';
SET allow_statistics = 1;
SET use_statistics = 1;
SET materialize_statistics_on_insert = 1;
SET allow_suspicious_low_cardinality_types = 1;
SET enable_analyzer = 1;
SET optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1;
SET allow_reorder_prewhere_conditions = 1;
SET move_all_conditions_to_prewhere = 1;
SET max_threads = 1;
SET min_insert_block_size_rows = 0, min_insert_block_size_bytes = 0;

DROP TABLE IF EXISTS t_countmin_reuse_src;
DROP TABLE IF EXISTS t_countmin_reuse_dense;
DROP TABLE IF EXISTS t_countmin_reuse_sparse;

CREATE TABLE t_countmin_reuse_src
(
    id UInt64,
    value LowCardinality(UInt32),
    selective_probe UInt8
) ENGINE = Memory;

CREATE TABLE t_countmin_reuse_dense
(
    value LowCardinality(UInt32) STATISTICS(countmin),
    selective_probe UInt8 STATISTICS(countmin)
) ENGINE = MergeTree ORDER BY tuple()
SETTINGS auto_statistics_types = '', refresh_statistics_interval = 0,
    min_bytes_for_wide_part = 1000000000000, min_rows_for_wide_part = 1000000000000;

CREATE TABLE t_countmin_reuse_sparse
(
    value LowCardinality(UInt32) STATISTICS(countmin),
    selective_probe UInt8 STATISTICS(countmin)
) ENGINE = MergeTree ORDER BY tuple()
SETTINGS auto_statistics_types = '', refresh_statistics_interval = 0,
    min_bytes_for_wide_part = 1000000000000, min_rows_for_wide_part = 1000000000000;

INSERT INTO t_countmin_reuse_src
SELECT
    number,
    toUInt32(if(number < 1000, if(number < 400, 42, number % 400),
        multiIf(number % 6 < 3, 42, number % 6 < 5, 7, 199))),
    toUInt8(if(number < 1000, number < 20, number < 1020))
FROM numbers(1060) SETTINGS max_block_size = 1060;

-- 1000 rows and 400 dictionary values: dense counting with the new twofold
-- reuse cutoff, but not with the old tenfold cutoff. Frequencies are not uniform.
INSERT INTO t_countmin_reuse_dense
SELECT value, selective_probe FROM t_countmin_reuse_src WHERE id < 1000;

-- 60 rows referencing three values in the retained 400-entry dictionary.
-- Unlike a six-row slice, this completes sparse counting without early abort.
INSERT INTO t_countmin_reuse_sparse
SELECT value, selective_probe FROM t_countmin_reuse_src WHERE id >= 1000;

SELECT 'dense fixture distribution';
SELECT count(), uniqExact(value), countIf(value = 42), countIf(value = 399), countIf(selective_probe = 1)
FROM t_countmin_reuse_dense;

SELECT 'dense common value uses row frequency';
SELECT countIf(position(line, '__table1.selective_probe') > 0
    AND position(line, '__table1.value') > position(line, '__table1.selective_probe')) = 1
FROM
(
    SELECT extractAll(explain, 'Prewhere filter column: ([^\n]+)')[1] AS line
    FROM (EXPLAIN actions = 1 SELECT count() FROM t_countmin_reuse_dense WHERE value = 42 AND selective_probe = 1)
    WHERE explain LIKE '%Prewhere filter column%'
);

SELECT 'dense rare value is selective';
SELECT countIf(position(line, '__table1.value') > 0
    AND position(line, '__table1.selective_probe') > position(line, '__table1.value')) = 1
FROM
(
    SELECT extractAll(explain, 'Prewhere filter column: ([^\n]+)')[1] AS line
    FROM (EXPLAIN actions = 1 SELECT count() FROM t_countmin_reuse_dense WHERE value = 399 AND selective_probe = 1)
    WHERE explain LIKE '%Prewhere filter column%'
);

SELECT 'sparse fixture distribution';
SELECT count(), uniqExact(value), countIf(value = 42), countIf(value = 199), countIf(selective_probe = 1)
FROM t_countmin_reuse_sparse;

SELECT 'sparse common value uses row frequency';
SELECT countIf(position(line, '__table1.selective_probe') > 0
    AND position(line, '__table1.value') > position(line, '__table1.selective_probe')) = 1
FROM
(
    SELECT extractAll(explain, 'Prewhere filter column: ([^\n]+)')[1] AS line
    FROM (EXPLAIN actions = 1 SELECT count() FROM t_countmin_reuse_sparse WHERE value = 42 AND selective_probe = 1)
    WHERE explain LIKE '%Prewhere filter column%'
);

SELECT 'sparse rare value is selective';
SELECT countIf(position(line, '__table1.value') > 0
    AND position(line, '__table1.selective_probe') > position(line, '__table1.value')) = 1
FROM
(
    SELECT extractAll(explain, 'Prewhere filter column: ([^\n]+)')[1] AS line
    FROM (EXPLAIN actions = 1 SELECT count() FROM t_countmin_reuse_sparse WHERE value = 199 AND selective_probe = 1)
    WHERE explain LIKE '%Prewhere filter column%'
);

DROP TABLE t_countmin_reuse_sparse;
DROP TABLE t_countmin_reuse_dense;
DROP TABLE t_countmin_reuse_src;
