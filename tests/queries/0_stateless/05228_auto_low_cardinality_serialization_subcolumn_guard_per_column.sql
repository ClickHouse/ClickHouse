-- The `length` / `notEmpty` -> `.size` rewrite is skipped only for the columns that can actually be
-- stored with automatic `LowCardinality` serialization: a `String` column without a cardinality
-- statistic is never encoded, so it keeps the rewrite even when the feature is enabled on its table.
-- With the feature disabled again, only a column that is still encoded in some part keeps the guard.

SET allow_experimental_statistics = 1;
SET materialize_statistics_on_insert = 1;
SET enable_analyzer = 1;
SET optimize_functions_to_subcolumns = 1;
SET mutations_sync = 2;

DROP TABLE IF EXISTS t_auto_lc_guard_per_column;
CREATE TABLE t_auto_lc_guard_per_column
(
    id UInt64,
    body String,
    lc String STATISTICS(uniq)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS
    auto_statistics_types = 'basic',
    max_uniq_number_for_low_cardinality = 1000,
    ratio_of_defaults_for_sparse_serialization = 0.9,
    min_bytes_for_wide_part = 0;

SELECT 'feature enabled, column without a cardinality statistic: rewrite fires';
SELECT count() > 0 FROM (EXPLAIN QUERY TREE run_passes = 1 SELECT length(body) FROM t_auto_lc_guard_per_column) WHERE explain LIKE '%body.size%';

SELECT 'feature enabled, column with a cardinality statistic: rewrite is skipped';
SELECT count() FROM (EXPLAIN QUERY TREE run_passes = 1 SELECT length(lc) FROM t_auto_lc_guard_per_column) WHERE explain LIKE '%lc.size%';

INSERT INTO t_auto_lc_guard_per_column SELECT number, 'body_' || toString(number), 'v_' || toString(number % 10) FROM numbers(2000);

SELECT 'kinds of the columns';
SELECT column, serialization_kind FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_auto_lc_guard_per_column' AND active AND column IN ('body', 'lc')
ORDER BY column;

SELECT 'correctness';
SELECT sum(length(body)), countIf(notEmpty(body)), sum(length(lc)), countIf(notEmpty(lc)) FROM t_auto_lc_guard_per_column;

-- The feature is off, but the encoded part still exists: only its column keeps the guard.
ALTER TABLE t_auto_lc_guard_per_column MODIFY SETTING max_uniq_number_for_low_cardinality = 0;

SELECT 'feature disabled, column without a cardinality statistic: rewrite fires';
SELECT count() > 0 FROM (EXPLAIN QUERY TREE run_passes = 1 SELECT length(body) FROM t_auto_lc_guard_per_column) WHERE explain LIKE '%body.size%';

SELECT 'feature disabled, encoded column: rewrite is skipped';
SELECT count() FROM (EXPLAIN QUERY TREE run_passes = 1 SELECT length(lc) FROM t_auto_lc_guard_per_column) WHERE explain LIKE '%lc.size%';

SELECT 'correctness';
SELECT sum(length(body)), countIf(notEmpty(body)), sum(length(lc)), countIf(notEmpty(lc)) FROM t_auto_lc_guard_per_column;

-- After the encoding is dropped from the part nothing is encoded, so both columns get the rewrite.
ALTER TABLE t_auto_lc_guard_per_column (REWRITE PARTS);

SELECT 'feature disabled, encoding dropped: kinds of the columns';
SELECT column, serialization_kind FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_auto_lc_guard_per_column' AND active AND column IN ('body', 'lc')
ORDER BY column;

SELECT 'feature disabled, encoding dropped: rewrite fires for both';
SELECT count() > 0 FROM (EXPLAIN QUERY TREE run_passes = 1 SELECT length(body) FROM t_auto_lc_guard_per_column) WHERE explain LIKE '%body.size%';
SELECT count() > 0 FROM (EXPLAIN QUERY TREE run_passes = 1 SELECT length(lc) FROM t_auto_lc_guard_per_column) WHERE explain LIKE '%lc.size%';

SELECT 'correctness';
SELECT sum(length(body)), countIf(notEmpty(body)), sum(length(lc)), countIf(notEmpty(lc)) FROM t_auto_lc_guard_per_column;

DROP TABLE t_auto_lc_guard_per_column;

-- The default `auto_statistics_types` adds a cardinality statistic to every eligible column, so a plain
-- `String` column of a table with the feature enabled is a candidate and keeps the guard.
DROP TABLE IF EXISTS t_auto_lc_guard_implicit;
CREATE TABLE t_auto_lc_guard_implicit
(
    id UInt64,
    s String
)
ENGINE = MergeTree
ORDER BY id
SETTINGS
    max_uniq_number_for_low_cardinality = 1000,
    ratio_of_defaults_for_sparse_serialization = 0.9,
    min_bytes_for_wide_part = 0;

SELECT 'implicit cardinality statistic: rewrite is skipped';
SELECT count() FROM (EXPLAIN QUERY TREE run_passes = 1 SELECT length(s) FROM t_auto_lc_guard_implicit) WHERE explain LIKE '%s.size%';

INSERT INTO t_auto_lc_guard_implicit SELECT number, 'v_' || toString(number % 10) FROM numbers(2000);

SELECT 'implicit cardinality statistic: kind, correctness';
SELECT DISTINCT serialization_kind FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_auto_lc_guard_implicit' AND active AND column = 's';
SELECT sum(length(s)), countIf(notEmpty(s)) FROM t_auto_lc_guard_implicit;

DROP TABLE t_auto_lc_guard_implicit;
