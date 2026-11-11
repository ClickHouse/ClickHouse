-- Two materialized views on the same source table writing to the same target,
-- plus a chained MV reading that target via a scalar subquery, used to abort the
-- server with a 'this->visited_views == right->visited_views' logical error:
-- parallel-insert squashing merges chunks from the two views (different visited_views)
-- while insert deduplication is disabled.
-- The INSERT below sets deduplicate_blocks_in_dependent_materialized_views=0 and
-- materialized_views_squash_parallel_inserts=1 to pin that precondition explicitly
-- (squashing is force-disabled whenever MV deduplication is on).

DROP TABLE IF EXISTS source;
DROP TABLE IF EXISTS source_null;
DROP TABLE IF EXISTS dest_a;

CREATE TABLE source (a Int32) ENGINE = MergeTree() ORDER BY tuple();
CREATE TABLE source_null AS source ENGINE = Null;
CREATE TABLE dest_a (count UInt64, count_subquery UInt64) ENGINE = MergeTree() ORDER BY tuple();

CREATE MATERIALIZED VIEW mv_null_1 TO source_null AS SELECT * FROM source;
CREATE MATERIALIZED VIEW mv_null_2 TO source_null AS SELECT * FROM source;
CREATE MATERIALIZED VIEW mv_a TO dest_a AS
SELECT count() AS count, (SELECT count() FROM source_null) AS count_subquery
FROM source_null;

INSERT INTO source SELECT number FROM numbers(2000)
SETTINGS min_insert_block_size_rows = 1500, max_insert_block_size = 1500,
         deduplicate_blocks_in_dependent_materialized_views = 0, materialized_views_squash_parallel_inserts = 1;

SELECT count() FROM source;

DROP TABLE mv_a;
DROP TABLE mv_null_2;
DROP TABLE mv_null_1;
DROP TABLE dest_a;
DROP TABLE source_null;
DROP TABLE source;
