-- The old default listed `parallel_hash`; the new one does not. `hash` and
-- `parallel_hash` must still physicalize the same HashJoin: same pipeline, same
-- plan actions, and the same parallel vs serial fill layout. This is not a
-- result check.
--
-- Join-order stats must stay on (`query_plan_optimize_join_order_limit` default
-- is 10) so MergeTree `totalRows` reaches `preferParallelHashLayout`. Pinning
-- that setting to 0 drops the estimate and both layouts collapse to parallel.

SET query_plan_optimize_join_order_randomize = 0;
SET query_plan_join_swap_table = 0;
SET allow_experimental_analyzer = 1;
SET enable_parallel_replicas = 0;
SET enable_join_runtime_filters = 0;
SET collect_hash_table_stats_during_joins = 0;
SET max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0;
SET explain_query_plan_default = 'legacy';
SET max_threads = 16;
-- clickhouse-test ships randomized `--max_threads` with every query, which overrides
-- session SET. Pin the knobs that decide parallel vs serial fill on the EXPLAIN itself.

SELECT 'default';
SELECT value FROM system.settings WHERE name = 'join_algorithm';

SET join_algorithm = 'parallel_hash';
SELECT value FROM system.settings WHERE name = 'join_algorithm';
SELECT count()
FROM system.warnings
WHERE message LIKE '%Obsolete setting%' AND message LIKE '%parallel_hash%';
SET join_algorithm = DEFAULT;

DROP TABLE IF EXISTS t05024_l;
DROP TABLE IF EXISTS t05024_r;
DROP TABLE IF EXISTS t05024_al;
DROP TABLE IF EXISTS t05024_ar;
CREATE TABLE t05024_l (n UInt64) ENGINE = MergeTree ORDER BY n;
CREATE TABLE t05024_r (n UInt64) ENGINE = MergeTree ORDER BY n;
INSERT INTO t05024_l SELECT number FROM numbers(100);
INSERT INTO t05024_r SELECT number FROM numbers(200);

SELECT 'pipeline_inner_parallel';
SET parallel_hash_join_threshold = 1;
SELECT equals(
    (SELECT groupArray(explain) FROM (
        EXPLAIN PIPELINE
        SELECT t1.n FROM t05024_l AS t1 INNER JOIN t05024_r AS t2 ON t1.n = t2.n
        SETTINGS join_algorithm = 'direct,hash,ie_join', max_threads = 16, query_plan_join_shard_by_pk_ranges = 0, query_plan_optimize_join_order_limit = 10, query_plan_optimize_join_order_randomize = 0
    )),
    (SELECT groupArray(explain) FROM (
        EXPLAIN PIPELINE
        SELECT t1.n FROM t05024_l AS t1 INNER JOIN t05024_r AS t2 ON t1.n = t2.n
        SETTINGS join_algorithm = 'direct,parallel_hash,hash,ie_join', max_threads = 16, query_plan_join_shard_by_pk_ranges = 0, query_plan_optimize_join_order_limit = 10, query_plan_optimize_join_order_randomize = 0
    )));
SELECT equals(
    (SELECT groupArray(explain) FROM (
        EXPLAIN PIPELINE
        SELECT t1.n FROM t05024_l AS t1 INNER JOIN t05024_r AS t2 ON t1.n = t2.n
        SETTINGS join_algorithm = 'hash', max_threads = 16, query_plan_join_shard_by_pk_ranges = 0, query_plan_optimize_join_order_limit = 10, query_plan_optimize_join_order_randomize = 0
    )),
    (SELECT groupArray(explain) FROM (
        EXPLAIN PIPELINE
        SELECT t1.n FROM t05024_l AS t1 INNER JOIN t05024_r AS t2 ON t1.n = t2.n
        SETTINGS join_algorithm = 'parallel_hash', max_threads = 16, query_plan_join_shard_by_pk_ranges = 0, query_plan_optimize_join_order_limit = 10, query_plan_optimize_join_order_randomize = 0
    )));
SELECT countIf(explain LIKE '%FillingRightJoinSide%')
FROM (
    EXPLAIN PIPELINE
    SELECT t1.n FROM t05024_l AS t1 INNER JOIN t05024_r AS t2 ON t1.n = t2.n
    SETTINGS join_algorithm = 'direct,hash,ie_join', max_threads = 16, query_plan_join_shard_by_pk_ranges = 0, query_plan_optimize_join_order_limit = 10, query_plan_optimize_join_order_randomize = 0
);

SELECT 'plan_inner_parallel';
SELECT equals(
    (SELECT groupArray(explain) FROM (
        EXPLAIN PLAN actions = 1
        SELECT t1.n FROM t05024_l AS t1 INNER JOIN t05024_r AS t2 ON t1.n = t2.n
        SETTINGS join_algorithm = 'direct,hash,ie_join', max_threads = 16, query_plan_join_shard_by_pk_ranges = 0, query_plan_optimize_join_order_limit = 10, query_plan_optimize_join_order_randomize = 0
    )),
    (SELECT groupArray(explain) FROM (
        EXPLAIN PLAN actions = 1
        SELECT t1.n FROM t05024_l AS t1 INNER JOIN t05024_r AS t2 ON t1.n = t2.n
        SETTINGS join_algorithm = 'direct,parallel_hash,hash,ie_join', max_threads = 16, query_plan_join_shard_by_pk_ranges = 0, query_plan_optimize_join_order_limit = 10, query_plan_optimize_join_order_randomize = 0
    )));

SELECT 'pipeline_right_parallel';
SELECT equals(
    (SELECT groupArray(explain) FROM (
        EXPLAIN PIPELINE
        SELECT t1.n, t2.n FROM t05024_l AS t1 RIGHT JOIN t05024_r AS t2 ON t1.n = t2.n
        SETTINGS join_algorithm = 'direct,hash,ie_join', max_threads = 16, query_plan_join_shard_by_pk_ranges = 0, query_plan_optimize_join_order_limit = 10, query_plan_optimize_join_order_randomize = 0, join_use_nulls = 1
    )),
    (SELECT groupArray(explain) FROM (
        EXPLAIN PIPELINE
        SELECT t1.n, t2.n FROM t05024_l AS t1 RIGHT JOIN t05024_r AS t2 ON t1.n = t2.n
        SETTINGS join_algorithm = 'direct,parallel_hash,hash,ie_join', max_threads = 16, query_plan_join_shard_by_pk_ranges = 0, query_plan_optimize_join_order_limit = 10, query_plan_optimize_join_order_randomize = 0, join_use_nulls = 1
    )));

DROP TABLE IF EXISTS t05024_al;
DROP TABLE IF EXISTS t05024_ar;
CREATE TABLE t05024_al (n UInt64, ts DateTime) ENGINE = MergeTree ORDER BY n;
CREATE TABLE t05024_ar (n UInt64, ts DateTime) ENGINE = MergeTree ORDER BY n;
INSERT INTO t05024_al SELECT number, toDateTime(number) FROM numbers(100);
INSERT INTO t05024_ar SELECT number, toDateTime(number) FROM numbers(200);

SELECT 'pipeline_asof_parallel';
-- ASOF is `JoinKind::Left` here. `preferParallelHashLayout` keys off kind, so ASOF (and SEMI / ANTI)
-- use the parallel layout when the threshold allows it.
SELECT equals(
    (SELECT groupArray(explain) FROM (
        EXPLAIN PIPELINE
        SELECT t1.n FROM t05024_al AS t1 ASOF LEFT JOIN t05024_ar AS t2 ON t1.n = t2.n AND t1.ts >= t2.ts
        SETTINGS join_algorithm = 'direct,hash,ie_join', max_threads = 16, query_plan_join_shard_by_pk_ranges = 0, query_plan_optimize_join_order_limit = 10, query_plan_optimize_join_order_randomize = 0
    )),
    (SELECT groupArray(explain) FROM (
        EXPLAIN PIPELINE
        SELECT t1.n FROM t05024_al AS t1 ASOF LEFT JOIN t05024_ar AS t2 ON t1.n = t2.n AND t1.ts >= t2.ts
        SETTINGS join_algorithm = 'direct,parallel_hash,hash,ie_join', max_threads = 16, query_plan_join_shard_by_pk_ranges = 0, query_plan_optimize_join_order_limit = 10, query_plan_optimize_join_order_randomize = 0
    )));

SELECT 'pipeline_inner_serial';
SET parallel_hash_join_threshold = 100000;
SELECT equals(
    (SELECT groupArray(explain) FROM (
        EXPLAIN PIPELINE
        SELECT t1.n FROM t05024_l AS t1 INNER JOIN t05024_r AS t2 ON t1.n = t2.n
        SETTINGS join_algorithm = 'direct,hash,ie_join', max_threads = 16, query_plan_join_shard_by_pk_ranges = 0, query_plan_optimize_join_order_limit = 10, query_plan_optimize_join_order_randomize = 0
    )),
    (SELECT groupArray(explain) FROM (
        EXPLAIN PIPELINE
        SELECT t1.n FROM t05024_l AS t1 INNER JOIN t05024_r AS t2 ON t1.n = t2.n
        SETTINGS join_algorithm = 'direct,parallel_hash,hash,ie_join', max_threads = 16, query_plan_join_shard_by_pk_ranges = 0, query_plan_optimize_join_order_limit = 10, query_plan_optimize_join_order_randomize = 0
    )));
SELECT equals(
    (SELECT groupArray(explain) FROM (
        EXPLAIN PIPELINE
        SELECT t1.n FROM t05024_l AS t1 INNER JOIN t05024_r AS t2 ON t1.n = t2.n
        SETTINGS join_algorithm = 'hash', max_threads = 16, query_plan_join_shard_by_pk_ranges = 0, query_plan_optimize_join_order_limit = 10, query_plan_optimize_join_order_randomize = 0
    )),
    (SELECT groupArray(explain) FROM (
        EXPLAIN PIPELINE
        SELECT t1.n FROM t05024_l AS t1 INNER JOIN t05024_r AS t2 ON t1.n = t2.n
        SETTINGS join_algorithm = 'parallel_hash', max_threads = 16, query_plan_join_shard_by_pk_ranges = 0, query_plan_optimize_join_order_limit = 10, query_plan_optimize_join_order_randomize = 0
    )));
SELECT countIf(explain LIKE '%FillingRightJoinSide%')
FROM (
    EXPLAIN PIPELINE
    SELECT t1.n FROM t05024_l AS t1 INNER JOIN t05024_r AS t2 ON t1.n = t2.n
    SETTINGS join_algorithm = 'direct,hash,ie_join', max_threads = 16, query_plan_join_shard_by_pk_ranges = 0, query_plan_optimize_join_order_limit = 10, query_plan_optimize_join_order_randomize = 0
);

SELECT 'plan_inner_serial';
SELECT equals(
    (SELECT groupArray(explain) FROM (
        EXPLAIN PLAN actions = 1
        SELECT t1.n FROM t05024_l AS t1 INNER JOIN t05024_r AS t2 ON t1.n = t2.n
        SETTINGS join_algorithm = 'direct,hash,ie_join', max_threads = 16, query_plan_join_shard_by_pk_ranges = 0, query_plan_optimize_join_order_limit = 10, query_plan_optimize_join_order_randomize = 0
    )),
    (SELECT groupArray(explain) FROM (
        EXPLAIN PLAN actions = 1
        SELECT t1.n FROM t05024_l AS t1 INNER JOIN t05024_r AS t2 ON t1.n = t2.n
        SETTINGS join_algorithm = 'direct,parallel_hash,hash,ie_join', max_threads = 16, query_plan_join_shard_by_pk_ranges = 0, query_plan_optimize_join_order_limit = 10, query_plan_optimize_join_order_randomize = 0
    )));

DROP TABLE t05024_l;
DROP TABLE t05024_r;
DROP TABLE t05024_al;
DROP TABLE t05024_ar;
