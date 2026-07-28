-- Test: exercises `findKey` path of `HashMethodBase` with `nullable=true` and
-- `consecutive_keys_optimization=true`. PR 61393 added cache.is_null tracking
-- in `findKey` (ColumnsHashingImpl.h:229-236) but the PR's own test 03009 only
-- exercises `emplaceKey`. The `findKey` path is reached when GROUP BY runs into
-- `max_rows_to_group_by` with `group_by_overflow_mode = 'any'`, after which
-- `Aggregator::executeImplBatch` switches to `findKey` for subsequent blocks.
-- Covers: src/Common/ColumnsHashingImpl.h:229-241 — findKey nullable branch.


DROP TABLE IF EXISTS t_nullable_findkey;

CREATE TABLE t_nullable_findkey (id UInt64, x Nullable(Int64))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 4;

SYSTEM STOP MERGES t_nullable_findkey;

-- Block 1 (rows 1-4) populates {1, NULL, 2}; result_size becomes 3.
-- Block 2 (rows 5-8) adds {3}; checkLimits sees 4>3 → no_more_keys=true.
-- Blocks 3+ use the findKey path. Mixed NULL/value rows in blocks 3+ exercise
-- the new findKey nullable+consecutive_keys cache logic.
INSERT INTO t_nullable_findkey VALUES
    (1, 1), (2, 1), (3, NULL), (4, 2),     -- block 1
    (5, 2), (6, NULL), (7, NULL), (8, 3),  -- block 2
    (9, 4), (10, NULL), (11, NULL), (12, 5),  -- block 3 (findKey)
    (13, 1), (14, 1), (15, NULL), (16, NULL); -- block 4 (findKey)

SELECT x, count(), countIf(x IS NULL) FROM t_nullable_findkey
GROUP BY x ORDER BY x
SETTINGS
    max_threads = 1,
    max_rows_to_group_by = 3,
    group_by_overflow_mode = 'any',
    max_block_size = 4,
    optimize_aggregation_in_order = 0,
    optimize_use_projections = 0,
    max_rows_to_read = 1000;

DROP TABLE t_nullable_findkey;
