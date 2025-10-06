-- Tags: no-debug, no-debug, no-asan, no-tsan, no-msan, no-ubsan, no-sanitize-coverage, no-parallel-replicas
-- - debug build adds CheckTokenTransform

SET max_threads=2;
SET max_insert_threads=2;
SET parallel_view_processing=1;

-- { echo }

CREATE TABLE 03633_mv_src (key Int) Engine=MergeTree ORDER BY ();
CREATE TABLE 03633_mv_dst (key Int) Engine=MergeTree ORDER BY ();
CREATE MATERIALIZED VIEW 03633_mv TO 03633_mv_dst AS SELECT * FROM 03633_mv_src;

SET deduplicate_blocks_in_dependent_materialized_views=0;
SET materialized_views_squash_parallel_inserts=1;
EXPLAIN PIPELINE INSERT INTO 03633_mv_src SELECT * FROM system.one;

SET deduplicate_blocks_in_dependent_materialized_views=0;
SET materialized_views_squash_parallel_inserts=0;
EXPLAIN PIPELINE INSERT INTO 03633_mv_src SELECT * FROM system.one;

SET deduplicate_blocks_in_dependent_materialized_views=1;
SET materialized_views_squash_parallel_inserts=1;
EXPLAIN PIPELINE INSERT INTO 03633_mv_src SELECT * FROM system.one;
