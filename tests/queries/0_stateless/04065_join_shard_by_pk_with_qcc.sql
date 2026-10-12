-- Tags: no-parallel
-- Regression test for wrong results in JOIN with shard-by-PK optimization and query condition cache.
-- The bug was in optimizeJoinByShards::apply() which assumed contiguous part_index_in_query values,
-- but filterPartsByQueryConditionCache can drop parts leaving gaps in the indices.
-- This caused the layer distribution to assign parts to the wrong source, producing 0 rows.

SYSTEM DROP QUERY CONDITION CACHE;

SET enable_join_runtime_filters = 0;
SET use_query_condition_cache = 1;
SET query_plan_join_shard_by_pk_ranges = 1;

DROP TABLE IF EXISTS t1_04065;
DROP TABLE IF EXISTS t2_04065;

-- auto_statistics_types forces the statistics-based join optimization path
-- Use tdigest (not countmin) because CountMinSketch requires datasketches library which is
-- disabled in the fast test build (-DENABLE_LIBRARIES=0).
CREATE TABLE t1_04065 (id UInt32, attr UInt32) ENGINE = MergeTree ORDER BY id
SETTINGS ratio_of_defaults_for_sparse_serialization = 0.1, auto_statistics_types = 'tdigest';

INSERT INTO t1_04065 VALUES (0, 0);

CREATE TABLE t2_04065 (id UInt32, attr UInt32) ENGINE = MergeTree ORDER BY id
SETTINGS ratio_of_defaults_for_sparse_serialization = 0.1, auto_statistics_types = 'tdigest';

INSERT INTO t2_04065 VALUES (0, 0);

-- First SELECT populates the query condition cache for t1's part with (0,0).
-- The prewhere condition attr != 0 evaluates to false, caching "no matching rows" for this part.
SELECT * FROM t1_04065 JOIN t2_04065 ON t1_04065.id = t2_04065.id AND t1_04065.attr != 0;

-- INSERT creates a new part in t1 with attr=1 (matches the condition).
INSERT INTO t1_04065 VALUES (0, 1);

-- Second SELECT must return the new row. Before the fix, the shard optimization
-- failed to assign the new part to t1's layer due to non-contiguous part indices.
SELECT * FROM t1_04065 JOIN t2_04065 ON t1_04065.id = t2_04065.id AND t1_04065.attr != 0;

DROP TABLE t1_04065;
DROP TABLE t2_04065;
