-- Random settings limits: use_join_disjunctions_push_down=(1, 1)
-- A JOIN under a WHERE that no row passes: each input the WHERE cannot be pushed to contributes
-- nothing, so it is replaced by an empty source and not read (issue #123702). An input that can
-- produce totals is kept, for this rule and for the constant-false ON short-circuit.

SET enable_parallel_replicas = 0;
SET query_plan_optimize_join_order_randomize = 0;

DROP TABLE IF EXISTS t_live;
DROP TABLE IF EXISTS t_small;
DROP TABLE IF EXISTS t_big;

CREATE TABLE t_live (d Date, c String) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_small (d Date, c String) ENGINE = MergeTree ORDER BY d;
CREATE TABLE t_big (c String, v UInt64) ENGINE = MergeTree ORDER BY c;

INSERT INTO t_live SELECT toDate('2026-01-01') + number, toString(number % 10) FROM numbers(100);
INSERT INTO t_small SELECT toDate('2026-01-01') + number, toString(number % 10) FROM numbers(100);
INSERT INTO t_big SELECT toString(number % 10), number FROM numbers(100000);

-- The outer WHERE is false for the second UNION ALL branch, whose constant `k` it reads.
SELECT 'LEFT JOIN', count()
FROM (SELECT d, 'b' AS k FROM t_live UNION ALL SELECT s.d AS d, 'x' AS k FROM t_small AS s LEFT JOIN t_big AS b ON s.c = b.c)
WHERE k = 'b' AND d >= '2026-02-01'
SETTINGS query_plan_join_swap_table = 'false', max_rows_to_read = 10000, read_overflow_mode = 'throw';

SELECT 'INNER JOIN', count()
FROM (SELECT d, 'b' AS k FROM t_live UNION ALL SELECT s.d AS d, 'x' AS k FROM t_small AS s INNER JOIN t_big AS b ON s.c = b.c)
WHERE k = 'b' AND d >= '2026-02-01'
SETTINGS query_plan_join_swap_table = 'false', max_rows_to_read = 10000, read_overflow_mode = 'throw';

SELECT 'FULL JOIN', count()
FROM t_small AS s FULL JOIN t_big AS b ON s.c = b.c
WHERE materialize(0)
SETTINGS query_plan_convert_outer_join_to_inner_join = 0, max_rows_to_read = 10000, read_overflow_mode = 'throw';

SELECT 'LEFT JOIN, aggregated right side', count()
FROM (SELECT d, 'b' AS k FROM t_live UNION ALL SELECT s.d AS d, 'x' AS k FROM t_small AS s LEFT JOIN (SELECT c, max(v) AS v FROM t_big GROUP BY c) AS b ON s.c = b.c)
WHERE k = 'b' AND d >= '2026-02-01'
SETTINGS query_plan_join_swap_table = 'false', max_rows_to_read = 10000, read_overflow_mode = 'throw';

SELECT 'plan: LEFT JOIN in the false branch';
SELECT count() FROM (
    EXPLAIN SELECT count()
    FROM (SELECT d, 'b' AS k FROM t_live UNION ALL SELECT s.d AS d, 'x' AS k FROM t_small AS s LEFT JOIN t_big AS b ON s.c = b.c)
    WHERE k = 'b' AND d >= '2026-02-01'
    SETTINGS query_plan_join_swap_table = 'false'
) WHERE explain ILIKE '%ReadNothing%';

SELECT 'plan: false WHERE over LEFT JOIN';
SELECT count() FROM (
    EXPLAIN SELECT * FROM t_small AS s LEFT JOIN t_big AS b ON s.c = b.c WHERE s.d >= '2026-02-01' AND materialize(0)
) WHERE explain ILIKE '%ReadNothing%';

SELECT 'plan: false WHERE with a side effect is evaluated, not short-circuited';
SELECT count() FROM (
    EXPLAIN SELECT * FROM t_small AS s LEFT JOIN t_big AS b ON s.c = b.c WHERE s.d >= '2026-02-01' AND materialize(0) AND sleepEachRow(0) = 0
) WHERE explain ILIKE '%ReadNothing%';

SELECT 'plan: false WHERE, right side WITH TOTALS is kept';
SELECT count() FROM (
    EXPLAIN SELECT * FROM t_small AS s LEFT JOIN (SELECT c, count() AS n FROM t_big GROUP BY c WITH TOTALS) AS b ON s.c = b.c
    WHERE s.d >= '2026-02-01' AND materialize(0)
) WHERE explain ILIKE '%ReadNothing%';

SELECT 'plan: false ON, right side WITH TOTALS is kept';
SELECT count() FROM (
    EXPLAIN SELECT * FROM (SELECT number % 2 AS k FROM numbers(4)) AS l
    LEFT JOIN (SELECT number % 3 AS k, count() AS c FROM numbers(6) GROUP BY k WITH TOTALS) AS r ON l.k = r.k AND 1 = 2
    SETTINGS query_plan_short_circuit_constant_false_join = 1
) WHERE explain ILIKE '%ReadNothing%';

SELECT 'plan: false ON, right side without totals';
SELECT count() FROM (
    EXPLAIN SELECT * FROM (SELECT number % 2 AS k FROM numbers(4)) AS l
    LEFT JOIN (SELECT number % 3 AS k, count() AS c FROM numbers(6) GROUP BY k) AS r ON l.k = r.k AND 1 = 2
    SETTINGS query_plan_short_circuit_constant_false_join = 1
) WHERE explain ILIKE '%ReadNothing%';

SELECT 'correlated subquery under a false WHERE';
SELECT number FROM numbers(3) AS t
WHERE materialize(0) AND number >= (SELECT count() FROM numbers(5) AS u WHERE u.number < t.number)
SETTINGS correlated_subqueries_use_in_memory_buffer = 1;

SELECT 'false ON keeps the totals row of the right side';
SELECT * FROM (SELECT number % 2 AS k FROM numbers(4)) AS l
LEFT JOIN (SELECT number % 3 AS k, count() AS c FROM numbers(6) GROUP BY k WITH TOTALS) AS r ON l.k = r.k AND 1 = 2
ORDER BY ALL
SETTINGS query_plan_join_swap_table = 'false', query_plan_short_circuit_constant_false_join = 1;

-- Each query runs twice: the first run writes the subquery result to the cache, the second reads it.
SELECT 'a right side read from the query result cache keeps its totals row';
SELECT * FROM (SELECT number % 2 AS k FROM numbers(4)) AS l
LEFT JOIN (SELECT v % 3 AS k, count() AS n FROM t_big GROUP BY k WITH TOTALS SETTINGS use_query_cache = 1) AS r ON l.k = r.k
WHERE materialize(0)
ORDER BY ALL
SETTINGS query_plan_join_swap_table = 'false', log_comment = '05320_cache_where_1';
SELECT * FROM (SELECT number % 2 AS k FROM numbers(4)) AS l
LEFT JOIN (SELECT v % 3 AS k, count() AS n FROM t_big GROUP BY k WITH TOTALS SETTINGS use_query_cache = 1) AS r ON l.k = r.k
WHERE materialize(0)
ORDER BY ALL
SETTINGS query_plan_join_swap_table = 'false', log_comment = '05320_cache_where_2';
SELECT * FROM (SELECT number % 2 AS k FROM numbers(4)) AS l
LEFT JOIN (SELECT v % 3 AS k, count() AS n FROM t_big GROUP BY k WITH TOTALS SETTINGS use_query_cache = 1) AS r ON l.k = r.k AND 1 = 2
ORDER BY ALL
SETTINGS query_plan_join_swap_table = 'false', query_plan_short_circuit_constant_false_join = 1, log_comment = '05320_cache_on_1';
SELECT * FROM (SELECT number % 2 AS k FROM numbers(4)) AS l
LEFT JOIN (SELECT v % 3 AS k, count() AS n FROM t_big GROUP BY k WITH TOTALS SETTINGS use_query_cache = 1) AS r ON l.k = r.k AND 1 = 2
ORDER BY ALL
SETTINGS query_plan_join_swap_table = 'false', query_plan_short_circuit_constant_false_join = 1, log_comment = '05320_cache_on_2';

SYSTEM FLUSH LOGS query_log;
SELECT log_comment, ProfileEvents['QueryCacheHits'] > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish'
    AND log_comment IN ('05320_cache_where_2', '05320_cache_on_2')
ORDER BY log_comment;

DROP TABLE t_live;
DROP TABLE t_small;
DROP TABLE t_big;
