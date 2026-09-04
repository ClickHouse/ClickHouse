-- Tags: no-old-analyzer

-- `used_number_of_joins` and the arrays that describe the joins count the joins of the `SELECT` of a
-- materialized view once, and not once per build of its pipeline, see
-- `05054_query_log_join_columns_view_rebuilds`. Two joins of one view must still be counted separately
-- when they look alike: what tells a join of a view apart from the other joins of that view is where it
-- sits in the pipeline, and not the columns that its inputs carry, which two joins of one query can share.
--
-- A `Join`-engine table is joined by a step that has the left side alone as its input, because the right
-- side is the table itself and not a pipeline, and the columns of the right sides are pruned here because
-- the view selects none of them. That leaves the two joins of the view with the same single input column,
-- `__table1.a UInt64`, so a join of this view cannot be recognized by the shape of its inputs.
--
-- `max_block_size` splits the `SELECT` of the `INSERT` into one block per row and the two
-- `min_insert_block_size_*` settings keep the squashing in front of the views from putting the blocks back
-- together, so that the pipeline of the view really is built more than once. That this is what happens is
-- asserted with `system.part_log`, because a single build would make the test pass for the wrong reason:
-- every build of the pipeline of the view writes a part of its own into the destination.

SET log_queries = 1;
-- The reported kind is the executed one, and the optimizer may execute a join with its sides swapped,
-- which reverses LEFT and RIGHT.
SET query_plan_join_swap_table = 0;

CREATE TABLE join_first (a UInt64, b UInt64) ENGINE = Join(ANY, LEFT, a);
CREATE TABLE join_second (a UInt64, c UInt64) ENGINE = Join(ANY, LEFT, a);
INSERT INTO join_first SELECT number, number FROM numbers(10);
INSERT INTO join_second SELECT number, number FROM numbers(10);

CREATE TABLE src (a UInt64) ENGINE = MergeTree ORDER BY a;
CREATE TABLE dst (a UInt64) ENGINE = MergeTree ORDER BY a;
CREATE MATERIALIZED VIEW mv TO dst AS
    SELECT src.a AS a FROM src
    ANY LEFT JOIN join_first ON src.a = join_first.a
    ANY LEFT JOIN join_second ON src.a = join_second.a;

INSERT INTO src SELECT number FROM numbers(10)
SETTINGS max_block_size = 1, min_insert_block_size_rows = 1, min_insert_block_size_bytes = 1,
         log_comment = '05055_same_shape_two_filled_joins';

SELECT count() FROM dst;

SYSTEM FLUSH LOGS part_log;
SELECT count() > 1 AS the_view_consumed_more_than_one_block
FROM system.part_log
WHERE database = currentDatabase() AND table = 'dst' AND event_type = 'NewPart';

SYSTEM FLUSH LOGS query_log;
SELECT log_comment, used_number_of_joins, used_join_algorithms, used_join_kinds, used_join_strictness
FROM system.query_log
WHERE current_database = currentDatabase()
  AND type = 'QueryFinish'
  AND event_date >= yesterday()
  AND log_comment = '05055_same_shape_two_filled_joins';

DROP TABLE mv;
DROP TABLE src;
DROP TABLE dst;
DROP TABLE join_first;
DROP TABLE join_second;
