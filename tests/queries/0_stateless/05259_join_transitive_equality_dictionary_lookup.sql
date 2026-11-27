-- A dictionary joined on its key keeps the direct key lookup when an equality implied through another
-- table spans its join step. A `CACHE` dictionary read in full returns only its cached keys, so a hash
-- join against a fresh one would match nothing.

SET enable_join_transitive_predicates = 1;
SET query_plan_optimize_join_order_algorithm = 'greedy';
SET query_plan_optimize_join_order_limit = 10;
SET query_plan_optimize_join_order_randomize = 0;
SET use_hash_table_stats_for_join_reordering = 0;
SET enable_parallel_replicas = 0;
SET join_algorithm = 'direct,hash';

DROP DICTIONARY IF EXISTS d_cache;
DROP TABLE IF EXISTS t_src;
DROP TABLE IF EXISTS t_a;
DROP TABLE IF EXISTS t_b;

CREATE TABLE t_src (k UInt64, v UInt32) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t_a (k UInt64, x UInt32) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t_b (v UInt32, w UInt32) ENGINE = MergeTree ORDER BY v;
INSERT INTO t_src VALUES (1, 10), (2, 20), (3, 99);
INSERT INTO t_a VALUES (1, 10), (2, 20), (3, 30);
INSERT INTO t_b VALUES (10, 1), (20, 2), (30, 3), (99, 4);
CREATE DICTIONARY d_cache (k UInt64, v UInt32) PRIMARY KEY k
    SOURCE(CLICKHOUSE(TABLE 't_src' DB currentDatabase())) LAYOUT(CACHE(SIZE_IN_CELLS 16)) LIFETIME(1000);

SELECT countIf(explain ILIKE '%Algorithm: DirectKeyValueJoin%')
FROM (EXPLAIN actions = 1 SELECT t_a.k, t_b.w FROM t_a JOIN d_cache ON t_a.k = d_cache.k JOIN t_b ON t_b.v = d_cache.v AND t_b.v = t_a.x);

SELECT t_a.k, t_b.w FROM t_a JOIN d_cache ON t_a.k = d_cache.k JOIN t_b ON t_b.v = d_cache.v AND t_b.v = t_a.x ORDER BY t_a.k;

DROP DICTIONARY d_cache;
DROP TABLE t_src;
DROP TABLE t_a;
DROP TABLE t_b;
