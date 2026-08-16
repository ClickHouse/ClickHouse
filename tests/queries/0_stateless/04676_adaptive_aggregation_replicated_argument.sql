-- Tags: long

DROP TABLE IF EXISTS t_adaptive_repl_left;
DROP TABLE IF EXISTS t_adaptive_repl_right;

CREATE TABLE t_adaptive_repl_left (k UInt64, g UInt64, s String, u UInt128, nv Nullable(UInt64))
    ENGINE = MergeTree ORDER BY k;
CREATE TABLE t_adaptive_repl_right (k UInt64) ENGINE = MergeTree ORDER BY k;

INSERT INTO t_adaptive_repl_left
SELECT number, number, concat('s_', toString(number)), toUInt128(number) * 7, if(number % 5 = 0, NULL, number * 3)
FROM numbers(400000);

-- One key window in eight matches three right rows instead of one. A block that grows keeps its
-- left columns lazily replicated while a block that does not is materialized, so the aggregation
-- receives a mix of replicated and dense argument columns at the same position.
INSERT INTO t_adaptive_repl_right SELECT number FROM numbers(400000);
INSERT INTO t_adaptive_repl_right SELECT number FROM numbers(400000) WHERE intDiv(number, 2048) % 8 = 0;
INSERT INTO t_adaptive_repl_right SELECT number FROM numbers(400000) WHERE intDiv(number, 2048) % 8 = 0;
OPTIMIZE TABLE t_adaptive_repl_right FINAL;

-- Each aggregate is computed twice: once through the adaptive aggregator and once through the
-- baseline one, so the pair also checks the coalesced values and not only the absence of an abort.
-- enable_lazy_columns_replication is randomized by the test runner and is what produces the mix,
-- so every load-bearing setting is pinned per query.

SELECT 'String';
SELECT sum(cityHash64(g, m)) FROM (
    SELECT l.g AS g, max(l.s) AS m FROM t_adaptive_repl_left AS l
    JOIN t_adaptive_repl_right AS r ON l.k = r.k GROUP BY l.g)
SETTINGS enable_adaptive_aggregator = 1, adaptive_aggregator_freeze_threshold = 0,
         enable_lazy_columns_replication = 1, max_threads = 4, max_block_size = 4096,
         max_rows_to_group_by = 0, query_plan_join_swap_table = 0;
SELECT sum(cityHash64(g, m)) FROM (
    SELECT l.g AS g, max(l.s) AS m FROM t_adaptive_repl_left AS l
    JOIN t_adaptive_repl_right AS r ON l.k = r.k GROUP BY l.g)
SETTINGS enable_adaptive_aggregator = 0,
         enable_lazy_columns_replication = 1, max_threads = 4, max_block_size = 4096,
         max_rows_to_group_by = 0, query_plan_join_swap_table = 0;

SELECT 'UInt128';
SELECT sum(cityHash64(g, m)) FROM (
    SELECT l.g AS g, max(l.u) AS m FROM t_adaptive_repl_left AS l
    JOIN t_adaptive_repl_right AS r ON l.k = r.k GROUP BY l.g)
SETTINGS enable_adaptive_aggregator = 1, adaptive_aggregator_freeze_threshold = 0,
         enable_lazy_columns_replication = 1, max_threads = 4, max_block_size = 4096,
         max_rows_to_group_by = 0, query_plan_join_swap_table = 0;
SELECT sum(cityHash64(g, m)) FROM (
    SELECT l.g AS g, max(l.u) AS m FROM t_adaptive_repl_left AS l
    JOIN t_adaptive_repl_right AS r ON l.k = r.k GROUP BY l.g)
SETTINGS enable_adaptive_aggregator = 0,
         enable_lazy_columns_replication = 1, max_threads = 4, max_block_size = 4096,
         max_rows_to_group_by = 0, query_plan_join_swap_table = 0;

SELECT 'Nullable(UInt64)';
SELECT sum(cityHash64(g, toString(m))) FROM (
    SELECT l.g AS g, max(l.nv) AS m FROM t_adaptive_repl_left AS l
    JOIN t_adaptive_repl_right AS r ON l.k = r.k GROUP BY l.g)
SETTINGS enable_adaptive_aggregator = 1, adaptive_aggregator_freeze_threshold = 0,
         enable_lazy_columns_replication = 1, max_threads = 4, max_block_size = 4096,
         max_rows_to_group_by = 0, query_plan_join_swap_table = 0;
SELECT sum(cityHash64(g, toString(m))) FROM (
    SELECT l.g AS g, max(l.nv) AS m FROM t_adaptive_repl_left AS l
    JOIN t_adaptive_repl_right AS r ON l.k = r.k GROUP BY l.g)
SETTINGS enable_adaptive_aggregator = 0,
         enable_lazy_columns_replication = 1, max_threads = 4, max_block_size = 4096,
         max_rows_to_group_by = 0, query_plan_join_swap_table = 0;

DROP TABLE t_adaptive_repl_left;
DROP TABLE t_adaptive_repl_right;
