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
SETTINGS index_granularity = 8, allow_nullable_key = 1;

-- One part with three granules of 8 rows each, sorted by `b`:
-- granule 0 = only `false`, granule 1 = only `true`, granule 2 = only NULL.
INSERT INTO bool_pk
SELECT multiIf(number < 8, false, number < 16, true, NULL), number
FROM numbers(24)
SETTINGS max_insert_threads = 1;

OPTIMIZE TABLE bool_pk FINAL;

-- Disable trivial-count optimisations so `SelectedMarks` reflects actual
-- granule reads (otherwise count-from-stats can hide pruning differences).
SELECT 'count'          AS predicate, count() FROM bool_pk                       SETTINGS enable_parallel_replicas = 0;
SELECT 'IS TRUE'        AS predicate, count() FROM bool_pk WHERE b IS TRUE       SETTINGS enable_parallel_replicas = 0, optimize_trivial_count_query = 0, optimize_use_implicit_projections = 0, log_comment = '04304 IS TRUE';
SELECT 'IS FALSE'       AS predicate, count() FROM bool_pk WHERE b IS FALSE      SETTINGS enable_parallel_replicas = 0, optimize_trivial_count_query = 0, optimize_use_implicit_projections = 0, log_comment = '04304 IS FALSE';
SELECT 'IS UNKNOWN'     AS predicate, count() FROM bool_pk WHERE b IS UNKNOWN    SETTINGS enable_parallel_replicas = 0, optimize_trivial_count_query = 0, optimize_use_implicit_projections = 0, log_comment = '04304 IS UNKNOWN';
SELECT 'IS NOT TRUE'    AS predicate, count() FROM bool_pk WHERE b IS NOT TRUE   SETTINGS enable_parallel_replicas = 0, optimize_trivial_count_query = 0, optimize_use_implicit_projections = 0, log_comment = '04304 IS NOT TRUE';
SELECT 'IS NOT FALSE'   AS predicate, count() FROM bool_pk WHERE b IS NOT FALSE  SETTINGS enable_parallel_replicas = 0, optimize_trivial_count_query = 0, optimize_use_implicit_projections = 0, log_comment = '04304 IS NOT FALSE';
SELECT 'IS NOT UNKNOWN' AS predicate, count() FROM bool_pk WHERE b IS NOT UNKNOWN SETTINGS enable_parallel_replicas = 0, optimize_trivial_count_query = 0, optimize_use_implicit_projections = 0, log_comment = '04304 IS NOT UNKNOWN';

SYSTEM FLUSH LOGS query_log;

-- `SelectedMarks` counts the granules read after primary-key pruning;
-- `SelectedMarksTotal` counts the granules considered before pruning.
-- `KeyCondition` recognises `isNull` and `isNotNull` (the lowered forms of
-- `IS UNKNOWN` and `IS NOT UNKNOWN`) and prunes one of the three granules
-- for both forms. The other four predicates lower to `isNotDistinctFrom` /
-- `isDistinctFrom`, which `KeyCondition` currently treats as the trivial
-- `true` condition, so no granule is dropped and `SelectedMarks` equals
-- `SelectedMarksTotal`.
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
