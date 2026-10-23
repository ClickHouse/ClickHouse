-- A hash join whose right side is known to read nothing (an empty `MergeTree` table, or an inner join with
-- such a table on either side) builds with the serial layout if the join then returns no rows: `INNER`,
-- `RIGHT` and `LEFT SEMI`. `LEFT`, `LEFT ANTI` and `FULL` joins still probe every left row, so they keep the
-- parallel layout. A serial build has one `FillingRightJoinSide`, a parallel one has `max_threads` of them.

SET max_threads = 4, max_threads_min_free_memory_per_thread = 0;
SET join_algorithm = 'hash', parallel_hash_join_threshold = 100000;
SET max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0;
SET min_joined_block_size_rows = 0, min_joined_block_size_bytes = 0;
SET enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0;
SET query_plan_join_swap_table = 0, query_plan_join_shard_by_pk_ranges = 0, query_plan_read_in_order_through_join = 0;
SET query_plan_optimize_join_order_limit = 0, query_plan_optimize_join_order_randomize = 0;
SET use_hash_table_stats_for_join_reordering = 0, collect_hash_table_stats_during_joins = 0;
SET enable_join_runtime_filters = 1, join_runtime_filter_min_probe_rows = 0, enable_join_transitive_predicates = 0;

CREATE TABLE t_l (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t_r (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t_e1 (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t_e2 (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t_e3 (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t_e4 (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_l SELECT number, number FROM numbers(10);
INSERT INTO t_r VALUES (1, 1);

SELECT 'inner, empty right';
SELECT trimLeft(explain) FROM (EXPLAIN PIPELINE SELECT * FROM t_l AS l INNER JOIN t_e1 AS r ON l.k = r.k)
WHERE explain LIKE '%FillingRightJoinSide%';

SELECT 'right, empty right';
SELECT trimLeft(explain) FROM (EXPLAIN PIPELINE SELECT * FROM t_l AS l RIGHT JOIN t_e1 AS r ON l.k = r.k)
WHERE explain LIKE '%FillingRightJoinSide%';

SELECT 'left semi, empty right';
SELECT trimLeft(explain) FROM (EXPLAIN PIPELINE SELECT * FROM t_l AS l LEFT SEMI JOIN t_e1 AS r ON l.k = r.k)
WHERE explain LIKE '%FillingRightJoinSide%';

SELECT 'left, empty right';
SELECT trimLeft(explain) FROM (EXPLAIN PIPELINE SELECT * FROM t_l AS l LEFT JOIN t_e1 AS r ON l.k = r.k)
WHERE explain LIKE '%FillingRightJoinSide%';

SELECT 'left anti, empty right';
SELECT trimLeft(explain) FROM (EXPLAIN PIPELINE SELECT * FROM t_l AS l LEFT ANTI JOIN t_e1 AS r ON l.k = r.k)
WHERE explain LIKE '%FillingRightJoinSide%';

SELECT 'full, empty right';
SELECT trimLeft(explain) FROM (EXPLAIN PIPELINE SELECT * FROM t_l AS l FULL JOIN t_e1 AS r ON l.k = r.k)
WHERE explain LIKE '%FillingRightJoinSide%';

SELECT 'inner, empty right, spilling hash join';
SELECT trimLeft(explain) FROM (EXPLAIN PIPELINE SELECT * FROM t_l AS l INNER JOIN t_e1 AS r ON l.k = r.k SETTINGS max_bytes_before_external_join = 1000000000)
WHERE explain LIKE '%FillingRightJoinSide%';

SELECT 'inner, right is an inner join of empty tables';
SELECT trimLeft(explain) FROM (EXPLAIN PIPELINE
    SELECT * FROM t_l AS l INNER JOIN (SELECT e1.k AS k FROM t_e1 AS e1 INNER JOIN t_e2 AS e2 ON e1.k = e2.k) AS x ON l.k = x.k)
WHERE explain LIKE '%FillingRightJoinSide%';

SELECT 'inner, right is a cross join of empty tables';
SELECT trimLeft(explain) FROM (EXPLAIN PIPELINE
    SELECT * FROM t_l AS l INNER JOIN (SELECT e1.k AS k FROM t_e1 AS e1 CROSS JOIN t_e2 AS e2) AS x ON l.k = x.k)
WHERE explain LIKE '%FillingRightJoinSide%';

SELECT 'inner, right is an inner join with an empty left side';
SELECT trimLeft(explain) FROM (EXPLAIN PIPELINE
    SELECT * FROM t_l AS l INNER JOIN (SELECT e.k AS k FROM t_e1 AS e INNER JOIN t_r AS r ON e.k = r.k) AS x ON l.k = x.k)
WHERE explain LIKE '%FillingRightJoinSide%';

SELECT 'inner, right is an inner join with an empty right side';
SELECT trimLeft(explain) FROM (EXPLAIN PIPELINE
    SELECT * FROM t_l AS l INNER JOIN (SELECT r.k AS k FROM t_r AS r INNER JOIN t_e1 AS e ON r.k = e.k) AS x ON l.k = x.k)
WHERE explain LIKE '%FillingRightJoinSide%';

SELECT 'inner, right is a left join with an empty right side';
SELECT trimLeft(explain) FROM (EXPLAIN PIPELINE
    SELECT * FROM t_l AS l INNER JOIN (SELECT r.k AS k FROM t_r AS r LEFT JOIN t_e1 AS e ON r.k = e.k) AS x ON l.k = x.k)
WHERE explain LIKE '%FillingRightJoinSide%';

SELECT 'inner, right is an aggregation of an empty table';
SELECT trimLeft(explain) FROM (EXPLAIN PIPELINE SELECT * FROM t_l AS l INNER JOIN (SELECT count() AS k FROM t_e1) AS x ON l.k = x.k)
WHERE explain LIKE '%FillingRightJoinSide%';

SELECT 'inner, non-empty right without an estimate';
SELECT trimLeft(explain) FROM (EXPLAIN PIPELINE SELECT * FROM t_l AS l INNER JOIN t_r AS r ON l.k = r.k)
WHERE explain LIKE '%FillingRightJoinSide%';

SELECT 'inner, empty right, parallel_hash_join_threshold = 0';
SELECT trimLeft(explain) FROM (EXPLAIN PIPELINE SELECT * FROM t_l AS l INNER JOIN t_e1 AS r ON l.k = r.k SETTINGS parallel_hash_join_threshold = 0)
WHERE explain LIKE '%FillingRightJoinSide%';

SELECT 'four empty tables, reordered';
SELECT trimLeft(explain) FROM (EXPLAIN PIPELINE
    SELECT * FROM t_e1 AS a
    INNER JOIN t_e2 AS b ON a.k = b.k
    INNER JOIN t_e3 AS c ON b.k = c.k
    INNER JOIN t_e4 AS d ON c.k = d.k
    SETTINGS query_plan_optimize_join_order_limit = 10, query_plan_optimize_join_order_algorithm = 'greedy')
WHERE explain LIKE '%FillingRightJoinSide%';
