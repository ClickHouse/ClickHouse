-- Recursive tuple flattening has no well-defined behaviour for a per-tuple null map, so a
-- Nullable Tuple column (at the top level or nested inside another Tuple) must be rejected at
-- table creation when `allow_tuple_element_aggregation` is enabled, instead of silently
-- producing wrong aggregation results. The same column is allowed when the setting is off, and a
-- Nullable wrapping a custom-named tuple (such as `Point`) is allowed because flattening keeps
-- custom-named tuples as opaque leaves.

SET allow_experimental_nullable_tuple_type = 1;

DROP TABLE IF EXISTS t_reject_top_level;
DROP TABLE IF EXISTS t_reject_nested;
DROP TABLE IF EXISTS t_reject_aggregating;
DROP TABLE IF EXISTS t_reject_coalescing;
DROP TABLE IF EXISTS t_allowed_when_disabled;
DROP TABLE IF EXISTS t_allowed_custom_named;

CREATE TABLE t_reject_top_level (id UInt32, t Nullable(Tuple(a UInt64, b UInt64)))
ENGINE = SummingMergeTree ORDER BY id
SETTINGS allow_tuple_element_aggregation = 1; -- { serverError NOT_IMPLEMENTED }

CREATE TABLE t_reject_nested (id UInt32, metrics Tuple(a UInt64, inner Nullable(Tuple(b UInt64))))
ENGINE = SummingMergeTree ORDER BY id
SETTINGS allow_tuple_element_aggregation = 1; -- { serverError NOT_IMPLEMENTED }

CREATE TABLE t_reject_aggregating (id UInt32, t Nullable(Tuple(v SimpleAggregateFunction(sum, UInt64))))
ENGINE = AggregatingMergeTree ORDER BY id
SETTINGS allow_tuple_element_aggregation = 1; -- { serverError NOT_IMPLEMENTED }

CREATE TABLE t_reject_coalescing (id UInt32, t Nullable(Tuple(a Nullable(UInt64))))
ENGINE = CoalescingMergeTree ORDER BY id
SETTINGS allow_tuple_element_aggregation = 1; -- { serverError NOT_IMPLEMENTED }

-- Allowed when the feature is disabled.
CREATE TABLE t_allowed_when_disabled (id UInt32, t Nullable(Tuple(a UInt64, b UInt64)))
ENGINE = SummingMergeTree ORDER BY id
SETTINGS allow_tuple_element_aggregation = 0;
SELECT count() FROM t_allowed_when_disabled;
DROP TABLE t_allowed_when_disabled;

-- Allowed with the feature enabled when the tuple is custom-named (Point), because custom-named
-- tuples are kept as opaque leaves and are not flattened.
CREATE TABLE t_allowed_custom_named (id UInt32, pt Nullable(Point))
ENGINE = SummingMergeTree ORDER BY id
SETTINGS allow_tuple_element_aggregation = 1;
SELECT count() FROM t_allowed_custom_named;
DROP TABLE t_allowed_custom_named;
