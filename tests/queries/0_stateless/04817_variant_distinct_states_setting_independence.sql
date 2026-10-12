-- The -Distinct combinator stores the distinct argument values and replays them into the nested function on
-- merge and finalization, so how the nested function treats the NULL rows of a Variant argument is part of the
-- meaning of an already written ...Distinct state. The replay is therefore setting-independent: the NULL keys a
-- history contains are always aggregated on replay (exactly like the versions that wrote states before the
-- NULL-skipping contract existed), and `aggregate_functions_skip_variant_nulls` only controls whether the NULL
-- rows of newly aggregated data enter the history.

SET allow_experimental_variant_type = 1;
SET allow_suspicious_variant_types = 1;

DROP TABLE IF EXISTS t_variant_distinct_src;
CREATE TABLE t_variant_distinct_src (v Variant(UInt64, String)) ENGINE = Memory;
INSERT INTO t_variant_distinct_src VALUES (NULL), (1::UInt64), ('a'), (1::UInt64);

-- Direct aggregation: the setting controls whether the NULL row enters the distinct-key history.
-- (countDistinct in a query is rewritten to `count_distinct_implementation` by the analyzer, so the genuine
-- -Distinct combinator over `count` is exercised through its -State form, which is not rewritten.)
SET aggregate_functions_skip_variant_nulls = 1;
SELECT 'skip', finalizeAggregation(countDistinctState(v)), arraySort(groupArrayDistinct(v)) FROM t_variant_distinct_src;
SET aggregate_functions_skip_variant_nulls = 0;
SELECT 'keep', finalizeAggregation(countDistinctState(v)), arraySort(groupArrayDistinct(v)) FROM t_variant_distinct_src;

-- Persisted states, written under both modes. countDistinct exercises the nested function whose creator
-- implements the NULL skipping itself (`count`), groupArrayDistinct exercises the ones covered by the
-- AggregateFunctionVariantNull wrapper.
DROP TABLE IF EXISTS t_variant_distinct_states;
CREATE TABLE t_variant_distinct_states
(
    tag String,
    c AggregateFunction(countDistinct, Variant(UInt64, String)),
    g AggregateFunction(groupArrayDistinct, Variant(UInt64, String))
) ENGINE = MergeTree ORDER BY tag;

SET aggregate_functions_skip_variant_nulls = 0;
INSERT INTO t_variant_distinct_states SELECT 'written with 0', countDistinctState(v), groupArrayDistinctState(v) FROM t_variant_distinct_src;
SET aggregate_functions_skip_variant_nulls = 1;
INSERT INTO t_variant_distinct_states SELECT 'written with 1', countDistinctState(v), groupArrayDistinctState(v) FROM t_variant_distinct_src;

-- A written state must read back to the same result under either value of the setting: the setting controlled
-- which rows entered the history at write time, and the replay of the history never depends on the reader's
-- setting. In particular, the state written with 0 contains the NULL key and keeps counting it.
SET aggregate_functions_skip_variant_nulls = 1;
SELECT 'read with 1', tag, countDistinctMerge(c), arraySort(groupArrayDistinctMerge(g)) FROM t_variant_distinct_states GROUP BY tag ORDER BY tag;
SET aggregate_functions_skip_variant_nulls = 0;
SELECT 'read with 0', tag, countDistinctMerge(c), arraySort(groupArrayDistinctMerge(g)) FROM t_variant_distinct_states GROUP BY tag ORDER BY tag;

DROP TABLE t_variant_distinct_states;
DROP TABLE t_variant_distinct_src;
