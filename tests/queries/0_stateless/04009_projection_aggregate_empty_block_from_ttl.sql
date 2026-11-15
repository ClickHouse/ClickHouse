-- Tags: no-random-merge-tree-settings

-- Aggregate projections with constant GROUP BY keys produce 1 row from 0 input rows.
-- When TTL deletes all rows during merge, the input block has 0 rows.
-- The projection must handle this gracefully instead of throwing a LOGICAL_ERROR.
--
-- Key conditions to reproduce:
-- 1. TTL with WHERE clause disables the `all_data_dropped` optimization in TTLTransform,
--    so the merge pipeline processes blocks normally instead of short-circuiting.
-- 2. Parts inserted BEFORE ADD PROJECTION lack the projection, forcing it to be rebuilt
--    (not merged) during OPTIMIZE, which calls calculateProjections on each block.
-- 3. All rows are already expired, so TTLDeleteAlgorithm removes all rows,
--    producing a 0-row block that reaches calculateProjections.
-- 4. max_number_of_merges_with_ttl_in_pool = 0 prevents background TTL merges from
--    emptying the parts before OPTIMIZE TABLE FINAL can merge them.

DROP TABLE IF EXISTS t_proj_ttl;

CREATE TABLE t_proj_ttl
(
    c0 DateTime DEFAULT '2000-01-01',
    c1 Int32
)
ENGINE = MergeTree
ORDER BY c1
TTL c0 + INTERVAL 1 SECOND DELETE WHERE c1 = c1
SETTINGS index_granularity = 4096, max_number_of_merges_with_ttl_in_pool = 0;

-- Insert data BEFORE adding the projection so these parts lack it
INSERT INTO t_proj_ttl (c1) SELECT number FROM numbers(100);
INSERT INTO t_proj_ttl (c1) SELECT number FROM numbers(100);
INSERT INTO t_proj_ttl (c1) SELECT number FROM numbers(100);

-- Add projection after inserts: old parts don't have it, forcing rebuild during merge
ALTER TABLE t_proj_ttl ADD PROJECTION p1 (SELECT NULL GROUP BY 0.674);

-- Force a merge which will rebuild the projection on TTL-deleted (empty) blocks
OPTIMIZE TABLE t_proj_ttl FINAL;

SELECT 'OK';

DROP TABLE t_proj_ttl;
