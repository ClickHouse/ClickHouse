-- Tags: no-old-analyzer

-- A pipeline that is rebuilt while the query runs - the relation of a `loop`, the recursive member of a
-- recursive CTE - plans itself again for every restart, and the analyzer evaluates the scalar subqueries
-- of a query while it plans it. A join inside such a scalar subquery must still be counted once for the
-- query, and not once per restart.
--
-- It is counted once because the analyzer keeps the value of a scalar subquery on the query context, see
-- `evaluateScalarSubqueryIfNeeded`, and every pipeline of one query shares that context: the subquery is
-- evaluated by the first plan that needs it and read from there by all the later ones, so its join
-- reaches the counters exactly once. This case pins that, because the counting of the rebuilt pipelines
-- themselves is arranged around the pipeline build rather than around the planning.
--
-- The row counts are the witness that the rebuilds really happened: the looped view holds three rows and
-- the query asks for ten, so the relation is restarted four times, and the recursive CTE runs five
-- iterations.

SET log_queries = 1;
-- The reported kind is the executed one, and the optimizer may execute a join with its sides swapped,
-- which reverses LEFT and RIGHT.
SET query_plan_join_swap_table = 0;

CREATE TABLE t1 (a UInt64) ENGINE = MergeTree ORDER BY a;
CREATE TABLE t2 (a UInt64) ENGINE = MergeTree ORDER BY a;
CREATE TABLE src (a UInt64) ENGINE = MergeTree ORDER BY a;
INSERT INTO t1 SELECT number FROM numbers(10);
INSERT INTO t2 SELECT number FROM numbers(10);
INSERT INTO src SELECT number FROM numbers(3);

CREATE VIEW v_scalar AS
    SELECT src.a AS a, (SELECT count() FROM t1 JOIN t2 ON t1.a = t2.a) AS c FROM src;

SELECT 'a join in a scalar subquery, in a relation restarted by the loop table function';
SELECT count() FROM (SELECT * FROM loop(v_scalar) LIMIT 10)
SETTINGS log_comment = '05233_scalar_a_loop', join_algorithm = 'hash';

SELECT 'the same view read once, for comparison';
SELECT count() FROM v_scalar
SETTINGS log_comment = '05233_scalar_b_single_read', join_algorithm = 'hash';

SELECT 'a join in a scalar subquery of the recursive member of a recursive CTE';
WITH RECURSIVE r AS (
    SELECT toUInt64(1) AS n
    UNION ALL
    SELECT r.n + toUInt64((SELECT count() FROM t1 JOIN t2 ON t1.a = t2.a) - 9) AS n FROM r WHERE r.n < 5
)
SELECT count() FROM r
SETTINGS log_comment = '05233_scalar_c_recursive', join_algorithm = 'hash';

SYSTEM FLUSH LOGS query_log;
SELECT log_comment, used_number_of_joins, used_join_algorithms, used_join_kinds, used_join_strictness
FROM system.query_log
WHERE current_database = currentDatabase()
  AND type = 'QueryFinish'
  AND event_date >= yesterday()
  AND log_comment LIKE '05233\_scalar\_%'
ORDER BY log_comment;

DROP TABLE v_scalar;
DROP TABLE src;
DROP TABLE t1;
DROP TABLE t2;
