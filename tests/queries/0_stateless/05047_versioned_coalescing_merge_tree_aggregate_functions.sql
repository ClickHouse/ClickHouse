-- AggregateFunction and SimpleAggregateFunction columns keep their own aggregation semantics,
-- as in SummingMergeTree and CoalescingMergeTree: their states are combined across all rows
-- of the group, the version does not apply to them.

SET optimize_on_insert = 0;

DROP TABLE IF EXISTS t_vcmt_agg;

CREATE TABLE t_vcmt_agg
(
    key UInt64,
    version UInt64,
    sum_state AggregateFunction(sum, UInt64),
    max_simple SimpleAggregateFunction(max, UInt64),
    val Nullable(UInt64)
)
ENGINE = VersionedCoalescingMergeTree(version)
ORDER BY key;

-- The row with the higher version is inserted first.
INSERT INTO t_vcmt_agg SELECT 1, 2, initializeAggregation('sumState', toUInt64(10)), 5, NULL;
INSERT INTO t_vcmt_agg SELECT 1, 1, initializeAggregation('sumState', toUInt64(7)), 9, 100;

OPTIMIZE TABLE t_vcmt_agg FINAL;

-- sum_state and max_simple aggregate over both versions, val respects the version.
SELECT key, version, finalizeAggregation(sum_state), max_simple, val FROM t_vcmt_agg;

DROP TABLE t_vcmt_agg;
