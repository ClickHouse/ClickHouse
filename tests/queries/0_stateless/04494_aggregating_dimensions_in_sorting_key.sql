-- https://github.com/ClickHouse/ClickHouse/issues/751
-- In AggregatingMergeTree, a column that is neither part of the sorting key nor an aggregate-state
-- measure would be silently collapsed during background merges, producing wrong results. Such schemas
-- are rejected at table creation unless `allow_dimensions_outside_sorting_key` is set.

DROP TABLE IF EXISTS t_agg_bad;
DROP TABLE IF EXISTS t_agg_ok;
DROP TABLE IF EXISTS t_agg_allowed;
DROP TABLE IF EXISTS t_agg_simple;
DROP TABLE IF EXISTS t_agg_partition;
DROP TABLE IF EXISTS t_agg_materialized;
DROP TABLE IF EXISTS t_agg_no_measure;
DROP TABLE IF EXISTS t_agg_tuple;
DROP TABLE IF EXISTS t_sum_explicit;
DROP TABLE IF EXISTS t_sum_auto;

-- AggregatingMergeTree: dimension `name` is outside the sorting key -> rejected.
CREATE TABLE t_agg_bad (key UInt64, name String, value AggregateFunction(sum, UInt64))
ENGINE = AggregatingMergeTree ORDER BY key; -- { serverError BAD_ARGUMENTS }

-- All dimensions are in the sorting key -> accepted.
CREATE TABLE t_agg_ok (key UInt64, name String, value AggregateFunction(sum, UInt64))
ENGINE = AggregatingMergeTree ORDER BY (key, name);
SELECT 'agg_ok created';

-- Explicit opt-out via the setting -> accepted.
CREATE TABLE t_agg_allowed (key UInt64, name String, value AggregateFunction(sum, UInt64))
ENGINE = AggregatingMergeTree ORDER BY key
SETTINGS allow_dimensions_outside_sorting_key = 1;
SELECT 'agg_allowed created';

-- A SimpleAggregateFunction column is a measure, not a dimension -> accepted even outside the key.
CREATE TABLE t_agg_simple (key UInt64, value SimpleAggregateFunction(sum, UInt64))
ENGINE = AggregatingMergeTree ORDER BY key;
SELECT 'agg_simple created';

-- A column that is constant within a part because it is in the partition key -> accepted.
CREATE TABLE t_agg_partition (key UInt64, part_col UInt64, value AggregateFunction(sum, UInt64))
ENGINE = AggregatingMergeTree PARTITION BY part_col ORDER BY key;
SELECT 'agg_partition created';

-- A MATERIALIZED column is computed (here from the sorting key), not a raw dimension, so it survives a
-- merge unchanged and must not be rejected as an off-key dimension -> accepted.
CREATE TABLE t_agg_materialized (key UInt64, mat String MATERIALIZED toString(key), value AggregateFunction(sum, UInt64))
ENGINE = AggregatingMergeTree ORDER BY key;
SELECT 'agg_materialized created';

-- The table has no aggregate-state column, so it is not aggregating anything (out of scope) -> accepted.
CREATE TABLE t_agg_no_measure (key UInt64, name String)
ENGINE = AggregatingMergeTree ORDER BY key;
SELECT 'agg_no_measure created';

-- With `allow_tuple_element_aggregation` a plain column can be a measure, so the check is skipped -> accepted.
CREATE TABLE t_agg_tuple (key UInt64, name String, value AggregateFunction(sum, UInt64))
ENGINE = AggregatingMergeTree ORDER BY key
SETTINGS allow_tuple_element_aggregation = 1;
SELECT 'agg_tuple created';

-- The check applies only to AggregatingMergeTree: SummingMergeTree is not affected, even with an
-- explicit list of columns to sum and a dimension (`name`) outside the sorting key -> accepted.
CREATE TABLE t_sum_explicit (key UInt64, name String, value UInt64)
ENGINE = SummingMergeTree(value) ORDER BY key;
SELECT 'sum_explicit created';

CREATE TABLE t_sum_auto (key UInt64, name String, value UInt64)
ENGINE = SummingMergeTree ORDER BY key;
SELECT 'sum_auto created';

DROP TABLE t_agg_ok;
DROP TABLE t_agg_allowed;
DROP TABLE t_agg_simple;
DROP TABLE t_agg_partition;
DROP TABLE t_agg_materialized;
DROP TABLE t_agg_no_measure;
DROP TABLE t_agg_tuple;
DROP TABLE t_sum_explicit;
DROP TABLE t_sum_auto;
