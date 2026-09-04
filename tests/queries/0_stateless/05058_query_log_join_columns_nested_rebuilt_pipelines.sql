-- Tags: no-old-analyzer

-- A pipeline that is rebuilt while the query runs can sit inside another one that is rebuilt too: the
-- recursive member of a recursive CTE is built once per iteration, and when that CTE belongs to the
-- `SELECT` of a materialized view the whole thing is built again for every block of the `INSERT`. The
-- join of the recursive member must still be counted for the shape of the query, so its count must not
-- grow with either the iterations or the blocks.
--
-- The count is asserted as a bound and not as an exact number, because how many times the scalar
-- subquery of the view is evaluated is not part of the contract this test pins: what matters is that the
-- number stays small instead of following the number of rebuilds. Without the deduplication this query
-- reports one join per iteration per block, which is far above the bound.
--
-- `max_block_size` and the two `min_insert_block_size_*` settings split the `INSERT` into one block per
-- row so that the view really is built more than once, which `system.part_log` asserts: every build
-- writes a part of its own into the destination.

SET log_queries = 1;
-- The reported kind is the executed one, and the optimizer may execute a join with its sides swapped,
-- which reverses LEFT and RIGHT.
SET query_plan_join_swap_table = 0;

CREATE TABLE dim (a UInt64) ENGINE = MergeTree ORDER BY a;
INSERT INTO dim SELECT number FROM numbers(100);

SELECT 'a recursive CTE with one join, in the SELECT of a materialized view';
CREATE TABLE src (a UInt64) ENGINE = MergeTree ORDER BY a;
CREATE TABLE dst (a UInt64, n UInt64) ENGINE = MergeTree ORDER BY a;
CREATE MATERIALIZED VIEW mv TO dst AS
    SELECT src.a AS a,
           (
               WITH RECURSIVE r AS (
                   SELECT toUInt64(1) AS n
                   UNION ALL
                   SELECT r.n + 1 AS n FROM r JOIN dim ON r.n = dim.a WHERE r.n < 5
               )
               SELECT count() FROM r
           ) AS n
    FROM src;

INSERT INTO src SELECT number FROM numbers(4)
SETTINGS max_block_size = 1, min_insert_block_size_rows = 1, min_insert_block_size_bytes = 1,
         log_comment = '05058_nested_recursive_cte_in_a_view', join_algorithm = 'hash';

SELECT count() FROM dst;

SYSTEM FLUSH LOGS part_log;
SELECT count() > 1 AS the_view_consumed_more_than_one_block
FROM system.part_log
WHERE database = currentDatabase() AND table = 'dst' AND event_type = 'NewPart';

SYSTEM FLUSH LOGS query_log;
SELECT log_comment,
       used_number_of_joins <= 2 AS the_count_does_not_follow_the_rebuilds,
       used_join_algorithms
FROM system.query_log
WHERE current_database = currentDatabase()
  AND type = 'QueryFinish'
  AND event_date >= yesterday()
  AND log_comment = '05058_nested_recursive_cte_in_a_view';

DROP TABLE mv;
DROP TABLE src;
DROP TABLE dst;
DROP TABLE dim;
