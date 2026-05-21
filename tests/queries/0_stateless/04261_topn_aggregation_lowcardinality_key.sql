-- Regression: `Bad cast from type DB::ColumnString to DB::ColumnLowCardinality`
-- (or numeric variants) when a GROUP BY key was `LowCardinality(T)` and
-- `optimize_topn_aggregation` was enabled. The accumulated key columns were
-- cloned from the unwrapped runtime columns; `convertToFullIfNeeded` strips
-- `LowCardinality` as well as `Const`/`Sparse`, so the output column type no
-- longer matched the `LowCardinality(T)` output header and tripped
-- `typeid_cast` inside `SerializationLowCardinality::serializeBinaryBulkWithMultipleStreams`.
-- Originally surfaced by the AST fuzzer on PR #98607.

SET allow_suspicious_low_cardinality_types = 1;

DROP TABLE IF EXISTS t_topn_lc_key;

CREATE TABLE t_topn_lc_key (`grp` LowCardinality(String), `val` UInt64)
ENGINE = MergeTree ORDER BY val;

INSERT INTO t_topn_lc_key
SELECT concat('group_', leftPad(toString(number % 100), 3, '0')),
       ((number % 100) * 100) + (number / 100)
FROM numbers(1000);

SELECT '-- LowCardinality(String) key, UInt64 aggregate (Mode 2)';
SELECT grp, max(val) AS m
FROM t_topn_lc_key
GROUP BY grp
ORDER BY m DESC
LIMIT 5
SETTINGS optimize_topn_aggregation = 1;

SELECT '-- same query with an unrelated constant SELECT column';
SELECT grp, toNullable(toNullable(100)), max(val) AS m
FROM t_topn_lc_key
GROUP BY grp
ORDER BY m DESC
LIMIT 5
SETTINGS optimize_topn_aggregation = 1;

DROP TABLE t_topn_lc_key;


DROP TABLE IF EXISTS t_topn_lc_both;

CREATE TABLE t_topn_lc_both (`grp` LowCardinality(String), `val` LowCardinality(UInt64))
ENGINE = MergeTree ORDER BY val
SETTINGS prewarm_mark_cache = 1, index_granularity = 8, remove_empty_parts = 0;

INSERT INTO t_topn_lc_both
SELECT concat('group_', leftPad(toString(number % 100), 3, '0')),
       ((number % 100) * 100) + (number / 100)
FROM numbers(1000);

SELECT '-- LowCardinality on both key and aggregate argument (Mode 2)';
SELECT DISTINCT grp, max(val) AS m
FROM t_topn_lc_both
GROUP BY grp
ORDER BY m DESC
LIMIT 5
SETTINGS optimize_topn_aggregation = 1;

SELECT '-- Mode 1 (sorted input) with LowCardinality key';
SELECT grp, max(val) AS m
FROM t_topn_lc_both
GROUP BY grp
ORDER BY m DESC
LIMIT 5
SETTINGS optimize_topn_aggregation = 1, max_threads = 1;

SELECT '-- cross-check: optimized vs unoptimized produce the same top-K';
SELECT count() FROM
(
    SELECT grp, max(val) AS m
    FROM t_topn_lc_both
    GROUP BY grp
    ORDER BY m DESC
    LIMIT 10
    SETTINGS optimize_topn_aggregation = 1
) AS opt
FULL OUTER JOIN
(
    SELECT grp, max(val) AS m
    FROM t_topn_lc_both
    GROUP BY grp
    ORDER BY m DESC
    LIMIT 10
    SETTINGS optimize_topn_aggregation = 0
) AS ref USING (grp, m);

DROP TABLE t_topn_lc_both;
