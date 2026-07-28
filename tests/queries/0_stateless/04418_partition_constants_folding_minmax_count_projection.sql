-- Tags: no-parallel-replicas

-- Covers per-partition constant folding in the implicit MinMax-count projection path
-- (`MergeTreeData::getMinMaxCountProjectionBlock`), where the partition predicate is folded
-- per part before checking the part's MinMax hyperrectangle. The aggregate result must stay
-- correct regardless of how much pruning the folding enables.

SET enable_analyzer = 1;
SET use_constant_folding_in_index_analysis = 1;
SET optimize_use_implicit_projections = 1;
SET use_statistics_for_part_pruning=0;

DROP TABLE IF EXISTS folding_minmax_count;

CREATE TABLE folding_minmax_count (a UInt64, b UInt64)
ENGINE = MergeTree
ORDER BY tuple()
PARTITION BY a
SETTINGS index_granularity = 1;

INSERT INTO folding_minmax_count VALUES (1, 10), (1, 11), (2, 20), (3, 30);

-- The predicate `intDiv(100, a) = 100` is true only for the partition `a = 1`; folding
-- specializes it per part so the MinMax-count projection can prune the other partitions.
SELECT 'folded predicate', count() FROM folding_minmax_count WHERE intDiv(100, a) = 100;

-- A disjunction across partitions, answered from the MinMax-count projection.
SELECT 'disjunction', count() FROM folding_minmax_count WHERE a = 2 OR a = 3;

-- No predicate: the projection answers count() directly.
SELECT 'all', count() FROM folding_minmax_count;

DROP TABLE folding_minmax_count;
