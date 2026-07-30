-- Verify primary-key pruning for the `IS TRUE` / `IS FALSE` / `IS UNKNOWN`
-- truth-value predicates and their `IS NOT` variants on a `Nullable(Bool)`
-- column used as the leading primary-key column.
-- Follow-up to https://github.com/ClickHouse/ClickHouse/pull/99997

DROP TABLE IF EXISTS bool_pk;

CREATE TABLE bool_pk
(
    b  Nullable(Bool),
    id UInt32
)
ENGINE = MergeTree
ORDER BY (b, id)
-- Pin the layout so the granule-count assertions below are stable: randomized
-- adaptive granularity (index_granularity_bytes) could split the table into a
-- different number of marks and break the hard-coded SelectedMarks counts.
SETTINGS index_granularity = 8, index_granularity_bytes = 0, min_bytes_for_wide_part = 0, allow_nullable_key = 1;

-- One part with three granules of 8 rows each, sorted by `b`:
-- granule 0 = only `false`, granule 1 = only `true`, granule 2 = only NULL.
INSERT INTO bool_pk
SELECT multiIf(number < 8, false, number < 16, true, NULL), number
FROM numbers(24)
SETTINGS max_insert_threads = 1;

OPTIMIZE TABLE bool_pk FINAL;

-- Disable trivial-count, implicit projections, and sparsity pruning so
-- `SelectedMarks` reflects the primary key alone; otherwise those paths
-- can bypass the read or drop additional granules and hide the PK assertion.
SET enable_parallel_replicas = 0,
    optimize_trivial_count_query = 0,
    optimize_use_implicit_projections = 0,
    optimize_trivial_count_with_sparsity_filter = 0;

SELECT 'count'          AS predicate, count() FROM bool_pk;
SELECT 'IS TRUE'        AS predicate, count() FROM bool_pk WHERE b IS TRUE        SETTINGS log_comment = '04304 IS TRUE';
SELECT 'IS FALSE'       AS predicate, count() FROM bool_pk WHERE b IS FALSE       SETTINGS log_comment = '04304 IS FALSE';
SELECT 'IS UNKNOWN'     AS predicate, count() FROM bool_pk WHERE b IS UNKNOWN     SETTINGS log_comment = '04304 IS UNKNOWN';
SELECT 'IS NOT TRUE'    AS predicate, count() FROM bool_pk WHERE b IS NOT TRUE    SETTINGS log_comment = '04304 IS NOT TRUE';
SELECT 'IS NOT FALSE'   AS predicate, count() FROM bool_pk WHERE b IS NOT FALSE   SETTINGS log_comment = '04304 IS NOT FALSE';
SELECT 'IS NOT UNKNOWN' AS predicate, count() FROM bool_pk WHERE b IS NOT UNKNOWN SETTINGS log_comment = '04304 IS NOT UNKNOWN';

SYSTEM FLUSH LOGS query_log;

-- `SelectedMarks` counts the granules read after primary-key pruning;
-- `SelectedMarksTotal` counts the granules considered before pruning.
-- `IS TRUE` / `IS FALSE` lower to `b <=> true` / `b <=> false`
-- (`isNotDistinctFrom` against a non-NULL constant), which `KeyCondition` maps
-- like `b = true` / `b = false` and prunes to the single matching granule.
-- `IS UNKNOWN` / `IS NOT UNKNOWN` lower to `isNull` / `isNotNull` and prune the
-- NULL granule. The `IS NOT TRUE` / `IS NOT FALSE` forms lower to `isDistinctFrom`,
-- which `KeyCondition` still treats as the trivial `true` condition, so no
-- granule is dropped and `SelectedMarks` equals `SelectedMarksTotal`.
SELECT
    splitByString('04304 ', log_comment)[2]  AS predicate,
    ProfileEvents['SelectedMarks']           AS granules_read,
    ProfileEvents['SelectedMarksTotal']      AS granules_total
FROM system.query_log
WHERE current_database = currentDatabase()
  AND log_comment LIKE '04304 %'
  AND type = 'QueryFinish'
ORDER BY predicate;

DROP TABLE bool_pk;

-- Wrapped `IS TRUE` forms must prune the same as the bare predicate. The analyzer lowers
-- `X IS TRUE` to `isNotDistinctFrom(X, true)`; when `X` is a non-semantic wrapper over a
-- boolean predicate (`materialize(k = 42)`, trivial `CAST(k = 42, 'UInt8')`), `KeyCondition`
-- peels the wrapper before the boolean-result gate so the inner `k = 42` becomes a key atom.
-- Assert the granule pruning is preserved (1 of 10 granules) via `EXPLAIN indexes = 1`.
DROP TABLE IF EXISTS int_pk;
CREATE TABLE int_pk (k UInt32) ENGINE = MergeTree ORDER BY k
SETTINGS index_granularity = 8, index_granularity_bytes = 0, min_bytes_for_wide_part = 0, add_minmax_index_for_numeric_columns = 0;
INSERT INTO int_pk SELECT number FROM numbers(80) SETTINGS max_insert_threads = 1;
OPTIMIZE TABLE int_pk FINAL;

SELECT 'k = 42',                        countIf(explain LIKE '%Granules: 1/10%') FROM (EXPLAIN indexes = 1 SELECT count() FROM int_pk WHERE k = 42);
SELECT '(k = 42) IS TRUE',             countIf(explain LIKE '%Granules: 1/10%') FROM (EXPLAIN indexes = 1 SELECT count() FROM int_pk WHERE (k = 42) IS TRUE);
SELECT 'CAST(k = 42, UInt8) IS TRUE',  countIf(explain LIKE '%Granules: 1/10%') FROM (EXPLAIN indexes = 1 SELECT count() FROM int_pk WHERE CAST(k = 42, 'UInt8') IS TRUE);
SELECT 'materialize(k = 42) IS TRUE',  countIf(explain LIKE '%Granules: 1/10%') FROM (EXPLAIN indexes = 1 SELECT count() FROM int_pk WHERE materialize(k = 42) IS TRUE);

-- Results must be unchanged by the pruning (one matching row each).
SELECT count() FROM int_pk WHERE (k = 42) IS TRUE                       SETTINGS optimize_trivial_count_query = 0;
SELECT count() FROM int_pk WHERE CAST(k = 42, 'UInt8') IS TRUE          SETTINGS optimize_trivial_count_query = 0;
SELECT count() FROM int_pk WHERE materialize(k = 42) IS TRUE           SETTINGS optimize_trivial_count_query = 0;

DROP TABLE int_pk;
