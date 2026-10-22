-- https://github.com/ClickHouse/ClickHouse/issues/120424
-- Under `join_use_nulls` a selected right join key is joined on as `toNullable(key)`, which for a
-- `LowCardinality` key is `LowCardinality(Nullable(T))`, so the `full_sorting_merge` key check has to
-- compare the key types with both wrappers removed.
-- Each merge case has a `hash` twin with the same expected rows, and the `hash` rows are the oracle;
-- case 1 is a control that passes without the fix and case 5 pins the output type.

SET join_use_nulls = 1;
SET enable_analyzer = 1;
SET allow_suspicious_low_cardinality_types = 1;

DROP TABLE IF EXISTS t_lc_l;
DROP TABLE IF EXISTS t_lc_r;

CREATE TABLE t_lc_l (k LowCardinality(String), v UInt32) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_lc_r (k LowCardinality(String), w UInt32) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_lc_l VALUES ('a', 1), ('b', 2), ('c', 3), ('c', 4);
INSERT INTO t_lc_r VALUES ('a', 10), ('c', 30), ('c', 31), ('d', 40);

SELECT '-- 1. LEFT, key not selected, full_sorting_merge';
SELECT l.k, r.w FROM t_lc_l AS l LEFT JOIN t_lc_r AS r ON l.k = r.k
ORDER BY ALL SETTINGS join_algorithm = 'full_sorting_merge', query_plan_join_swap_table = 0;

SELECT '-- 2. LEFT, key selected, swap 0, hash';
SELECT l.k, r.k, r.w FROM t_lc_l AS l LEFT JOIN t_lc_r AS r ON l.k = r.k
ORDER BY ALL SETTINGS join_algorithm = 'hash', query_plan_join_swap_table = 0;

SELECT '-- 2. LEFT, key selected, swap 0, full_sorting_merge';
SELECT l.k, r.k, r.w FROM t_lc_l AS l LEFT JOIN t_lc_r AS r ON l.k = r.k
ORDER BY ALL SETTINGS join_algorithm = 'full_sorting_merge', query_plan_join_swap_table = 0;

SELECT '-- 2. LEFT, key selected, swap 0, parallel_full_sorting_merge';
SELECT l.k, r.k, r.w FROM t_lc_l AS l LEFT JOIN t_lc_r AS r ON l.k = r.k
ORDER BY ALL SETTINGS join_algorithm = 'parallel_full_sorting_merge', query_plan_join_swap_table = 0, max_threads = 4;

SELECT '-- 2b. parallel_full_sorting_merge is sharded, full_sorting_merge is not';
-- The parallel rows above are the only coverage of hash-sharded execution, which is the one path
-- where a key pair the shards hashed inconsistently would lose matches silently instead of throwing.
-- `optimize_read_in_order` and `query_plan_join_shard_by_pk_ranges` are randomized in CI, so pin them.
SELECT countIf(explain LIKE '%ScatterByPartitionTransform%') = 2
FROM (
    EXPLAIN PIPELINE
    SELECT l.k, r.k, r.w FROM t_lc_l AS l LEFT JOIN t_lc_r AS r ON l.k = r.k
    SETTINGS join_algorithm = 'parallel_full_sorting_merge', query_plan_join_swap_table = 0,
        max_threads = 4, optimize_read_in_order = 0, query_plan_join_shard_by_pk_ranges = 0
);
SELECT countIf(explain LIKE '%ScatterByPartitionTransform%') = 0
FROM (
    EXPLAIN PIPELINE
    SELECT l.k, r.k, r.w FROM t_lc_l AS l LEFT JOIN t_lc_r AS r ON l.k = r.k
    SETTINGS join_algorithm = 'full_sorting_merge', query_plan_join_swap_table = 0,
        max_threads = 4, optimize_read_in_order = 0, query_plan_join_shard_by_pk_ranges = 0
);

SELECT '-- 3. LEFT, key selected, swap 1, full_sorting_merge';
SELECT l.k, r.k, r.w FROM t_lc_l AS l LEFT JOIN t_lc_r AS r ON l.k = r.k
ORDER BY ALL SETTINGS join_algorithm = 'full_sorting_merge', query_plan_join_swap_table = 1;

SELECT '-- 4. FULL, key selected, swap 0, hash';
SELECT l.k, r.k, r.w FROM t_lc_l AS l FULL JOIN t_lc_r AS r ON l.k = r.k
ORDER BY ALL SETTINGS join_algorithm = 'hash', query_plan_join_swap_table = 0;

SELECT '-- 4. FULL, key selected, swap 0, full_sorting_merge';
SELECT l.k, r.k, r.w FROM t_lc_l AS l FULL JOIN t_lc_r AS r ON l.k = r.k
ORDER BY ALL SETTINGS join_algorithm = 'full_sorting_merge', query_plan_join_swap_table = 0;

SELECT '-- 4. FULL, key selected, swap 1, full_sorting_merge';
SELECT l.k, r.k, r.w FROM t_lc_l AS l FULL JOIN t_lc_r AS r ON l.k = r.k
ORDER BY ALL SETTINGS join_algorithm = 'full_sorting_merge', query_plan_join_swap_table = 1;

SELECT '-- 4. FULL, key selected, swap 0, parallel_full_sorting_merge';
SELECT l.k, r.k, r.w FROM t_lc_l AS l FULL JOIN t_lc_r AS r ON l.k = r.k
ORDER BY ALL SETTINGS join_algorithm = 'parallel_full_sorting_merge', query_plan_join_swap_table = 0, max_threads = 4;

SELECT '-- 5. LEFT, selected right key type, full_sorting_merge';
SELECT l.k, r.k, toTypeName(r.k) FROM t_lc_l AS l LEFT JOIN t_lc_r AS r ON l.k = r.k
ORDER BY ALL SETTINGS join_algorithm = 'full_sorting_merge', query_plan_join_swap_table = 0;

DROP TABLE t_lc_l;
DROP TABLE t_lc_r;

DROP TABLE IF EXISTS t_u16_l;
DROP TABLE IF EXISTS t_u16_r;

CREATE TABLE t_u16_l (k LowCardinality(UInt16), v UInt32) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_u16_r (k LowCardinality(UInt16), w UInt32) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_u16_l VALUES (1, 1), (2, 2), (3, 3);
INSERT INTO t_u16_r VALUES (1, 10), (3, 30);

SELECT '-- 6. LEFT, LowCardinality(UInt16) key, hash';
SELECT l.k, r.k, r.w FROM t_u16_l AS l LEFT JOIN t_u16_r AS r ON l.k = r.k
ORDER BY ALL SETTINGS join_algorithm = 'hash', query_plan_join_swap_table = 0;

SELECT '-- 6. LEFT, LowCardinality(UInt16) key, full_sorting_merge';
SELECT l.k, r.k, r.w FROM t_u16_l AS l LEFT JOIN t_u16_r AS r ON l.k = r.k
ORDER BY ALL SETTINGS join_algorithm = 'full_sorting_merge', query_plan_join_swap_table = 0;

DROP TABLE t_u16_l;
DROP TABLE t_u16_r;

DROP TABLE IF EXISTS t_asof_l;
DROP TABLE IF EXISTS t_asof_r;

CREATE TABLE t_asof_l (k LowCardinality(String), t UInt32) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_asof_r (k LowCardinality(String), t UInt32, w UInt32) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_asof_l VALUES ('a', 5), ('b', 7), ('c', 9);
INSERT INTO t_asof_r VALUES ('a', 1, 100), ('a', 4, 140), ('c', 2, 200);

SELECT '-- 7. ASOF LEFT, LowCardinality key, hash';
SELECT l.k, r.k, r.w FROM t_asof_l AS l ASOF LEFT JOIN t_asof_r AS r ON l.k = r.k AND l.t >= r.t
ORDER BY ALL SETTINGS join_algorithm = 'hash', query_plan_join_swap_table = 0;

SELECT '-- 7. ASOF LEFT, LowCardinality key, full_sorting_merge';
SELECT l.k, r.k, r.w FROM t_asof_l AS l ASOF LEFT JOIN t_asof_r AS r ON l.k = r.k AND l.t >= r.t
ORDER BY ALL SETTINGS join_algorithm = 'full_sorting_merge', query_plan_join_swap_table = 0;

DROP TABLE t_asof_l;
DROP TABLE t_asof_r;
