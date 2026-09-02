-- Tags: no-old-analyzer

-- `used_number_of_joins` and the arrays that describe the joins count the joins of the query, and the
-- `SELECT` of a materialized view has its pipeline built once for every block of the `INSERT` that
-- triggers it and once for every insert stream, all of these builds reporting into the counters of the
-- `INSERT`. A join of a view must be counted once all the same, no matter how many blocks the `INSERT`
-- was split into.
--
-- `max_block_size` splits the `SELECT` of the `INSERT` into one block per row and the two
-- `min_insert_block_size_*` settings keep the squashing in front of the views from putting the blocks
-- back together, so that the view really does consume more than one block. That this is what happens
-- is asserted with `system.part_log`, because a single block would make the test pass for the wrong
-- reason: every build of the pipeline of the view writes a part of its own into the destination.
--
-- Every case has a source table of its own, so that the blocks of one `INSERT` cannot be taken for
-- duplicates of the blocks of another one and be deduplicated away.
--
-- The algorithm is set explicitly, because the choice among the algorithms allowed by `join_algorithm`
-- is made at run time and depends on the number of threads.

SET log_queries = 1;
-- The reported kind is the executed one, and the optimizer may execute a join with its sides swapped,
-- which reverses LEFT and RIGHT.
SET query_plan_join_swap_table = 0;

CREATE TABLE dim1 (a UInt64) ENGINE = MergeTree ORDER BY a;
CREATE TABLE dim2 (a UInt64) ENGINE = MergeTree ORDER BY a;

INSERT INTO dim1 SELECT number FROM numbers(10);
INSERT INTO dim2 SELECT number FROM numbers(10);

SELECT 'one join in a materialized view, an insert of several blocks';
CREATE TABLE src_one (a UInt64) ENGINE = MergeTree ORDER BY a;
CREATE TABLE dst_one (a UInt64) ENGINE = MergeTree ORDER BY a;
CREATE MATERIALIZED VIEW mv_one TO dst_one AS SELECT src_one.a AS a FROM src_one JOIN dim1 ON src_one.a = dim1.a;

INSERT INTO src_one SELECT number FROM numbers(10)
SETTINGS max_block_size = 1, min_insert_block_size_rows = 1, min_insert_block_size_bytes = 1,
         log_comment = '05054_view_rebuilds_a_one_join', join_algorithm = 'hash';

SELECT count() FROM dst_one;

SYSTEM FLUSH LOGS part_log;
SELECT count() > 1 AS the_view_consumed_more_than_one_block
FROM system.part_log
WHERE database = currentDatabase() AND table = 'dst_one' AND event_type = 'NewPart';

SELECT 'one join in a materialized view, with parallel_view_processing enabled';
CREATE TABLE src_parallel (a UInt64) ENGINE = MergeTree ORDER BY a;
CREATE TABLE dst_parallel (a UInt64) ENGINE = MergeTree ORDER BY a;
CREATE MATERIALIZED VIEW mv_parallel TO dst_parallel AS
    SELECT src_parallel.a AS a FROM src_parallel JOIN dim1 ON src_parallel.a = dim1.a;

INSERT INTO src_parallel SELECT number FROM numbers(10)
SETTINGS max_block_size = 1, min_insert_block_size_rows = 1, min_insert_block_size_bytes = 1,
         max_insert_threads = 4, parallel_view_processing = 1,
         log_comment = '05054_view_rebuilds_b_parallel_view_processing', join_algorithm = 'hash';

SELECT count() FROM dst_parallel;

SELECT 'two joins in a materialized view, an insert of several blocks';
CREATE TABLE src_two (a UInt64) ENGINE = MergeTree ORDER BY a;
CREATE TABLE dst_two (a UInt64) ENGINE = MergeTree ORDER BY a;
CREATE MATERIALIZED VIEW mv_two TO dst_two AS
    SELECT src_two.a AS a FROM src_two JOIN dim1 ON src_two.a = dim1.a JOIN dim2 ON dim1.a = dim2.a;

INSERT INTO src_two SELECT number FROM numbers(10)
SETTINGS max_block_size = 1, min_insert_block_size_rows = 1, min_insert_block_size_bytes = 1,
         log_comment = '05054_view_rebuilds_c_two_joins', join_algorithm = 'hash';

SELECT count() FROM dst_two;

SELECT 'a join in each materialized view of a chain, an insert of several blocks';
CREATE TABLE src_chain (a UInt64) ENGINE = MergeTree ORDER BY a;
CREATE TABLE dst_chain_first (a UInt64) ENGINE = MergeTree ORDER BY a;
CREATE TABLE dst_chain_second (a UInt64) ENGINE = MergeTree ORDER BY a;
CREATE MATERIALIZED VIEW mv_chain_first TO dst_chain_first AS
    SELECT src_chain.a AS a FROM src_chain JOIN dim1 ON src_chain.a = dim1.a;
CREATE MATERIALIZED VIEW mv_chain_second TO dst_chain_second AS
    SELECT dst_chain_first.a AS a FROM dst_chain_first JOIN dim2 ON dst_chain_first.a = dim2.a;

INSERT INTO src_chain SELECT number FROM numbers(10)
SETTINGS max_block_size = 1, min_insert_block_size_rows = 1, min_insert_block_size_bytes = 1,
         log_comment = '05054_view_rebuilds_d_chain', join_algorithm = 'hash';

SELECT count() FROM dst_chain_second;

SELECT 'a join in the INSERT itself next to the join of the view';
-- The `SELECT` of the `INSERT` has its pipeline built once, and its join is counted next to the join of
-- the view: the joins of a view are deduplicated, the joins of the query that triggers it are not.
CREATE TABLE src_insert_join (a UInt64) ENGINE = MergeTree ORDER BY a;
CREATE TABLE dst_insert_join (a UInt64) ENGINE = MergeTree ORDER BY a;
CREATE MATERIALIZED VIEW mv_insert_join TO dst_insert_join AS
    SELECT src_insert_join.a AS a FROM src_insert_join JOIN dim1 ON src_insert_join.a = dim1.a;

INSERT INTO src_insert_join SELECT dim2.a FROM dim2 JOIN dim1 ON dim2.a = dim1.a
SETTINGS max_block_size = 1, min_insert_block_size_rows = 1, min_insert_block_size_bytes = 1,
         log_comment = '05054_view_rebuilds_e_insert_with_a_join', join_algorithm = 'hash';

SELECT count() FROM dst_insert_join;

SYSTEM FLUSH LOGS query_log;
SELECT log_comment, used_number_of_joins, used_join_algorithms, used_join_kinds, used_join_strictness
FROM system.query_log
WHERE current_database = currentDatabase()
  AND type = 'QueryFinish'
  AND event_date >= yesterday()
  AND log_comment LIKE '05054\_view\_rebuilds\_%'
ORDER BY log_comment;

DROP TABLE mv_one;
DROP TABLE mv_parallel;
DROP TABLE mv_two;
DROP TABLE mv_chain_first;
DROP TABLE mv_chain_second;
DROP TABLE mv_insert_join;
DROP TABLE src_one;
DROP TABLE src_parallel;
DROP TABLE src_two;
DROP TABLE src_chain;
DROP TABLE src_insert_join;
DROP TABLE dst_one;
DROP TABLE dst_parallel;
DROP TABLE dst_two;
DROP TABLE dst_chain_first;
DROP TABLE dst_chain_second;
DROP TABLE dst_insert_join;
DROP TABLE dim1;
DROP TABLE dim2;
