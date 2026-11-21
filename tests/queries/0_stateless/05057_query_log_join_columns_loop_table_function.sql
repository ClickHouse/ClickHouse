-- Tags: no-old-analyzer

-- `used_number_of_joins` and the arrays that describe the joins report the joins of the query, and the
-- `loop` table function restarts the relation it wraps every time that relation runs out of rows,
-- building its pipeline again for every pass and reporting into the counters of the same query. A join
-- of the looped relation must be counted once all the same, so that the counters describe the shape of
-- the query instead of how many rows it asked for.
--
-- The looped view holds three rows and the query asks for ten, so the relation is restarted four times.
-- The `count()` of ten is the witness that more than one pass ran: a single pass could return at most
-- the three rows of the view.
--
-- Two `loop` of the same view are two different joins and are counted apart, so the deduplication has to
-- name the `loop` and not the relation it wraps.
--
-- The algorithm is set explicitly, because the choice among the algorithms allowed by `join_algorithm`
-- is made at run time and depends on the number of threads.

SET log_queries = 1;
-- The reported kind is the executed one, and the optimizer may execute a join with its sides swapped,
-- which reverses LEFT and RIGHT.
SET query_plan_join_swap_table = 0;

CREATE TABLE dim (a UInt64) ENGINE = MergeTree ORDER BY a;
CREATE TABLE src (a UInt64) ENGINE = MergeTree ORDER BY a;
INSERT INTO dim SELECT number FROM numbers(3);
INSERT INTO src SELECT number FROM numbers(3);

CREATE VIEW v_with_join AS SELECT src.a AS a FROM src JOIN dim ON src.a = dim.a;

SELECT 'one join in a view restarted by the loop table function';
SELECT count() FROM (SELECT * FROM loop(v_with_join) LIMIT 10)
SETTINGS log_comment = '05057_loop_a_one_branch', join_algorithm = 'hash';

SELECT 'two loop branches over the same joined view';
SELECT count() FROM (
    SELECT * FROM loop(v_with_join) LIMIT 10
    UNION ALL
    SELECT * FROM loop(v_with_join) LIMIT 10
)
SETTINGS log_comment = '05057_loop_b_two_branches', join_algorithm = 'hash';

SYSTEM FLUSH LOGS query_log;
SELECT log_comment, used_number_of_joins, used_join_algorithms, used_join_kinds, used_join_strictness
FROM system.query_log
WHERE current_database = currentDatabase()
  AND type = 'QueryFinish'
  AND event_date >= yesterday()
  AND log_comment LIKE '05057\_loop\_%'
ORDER BY log_comment;

DROP TABLE v_with_join;
DROP TABLE src;
DROP TABLE dim;
