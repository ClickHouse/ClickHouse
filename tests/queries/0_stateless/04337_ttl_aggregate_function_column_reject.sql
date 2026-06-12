-- Verify that CREATE TABLE rejects TTL expressions referencing AggregateFunction columns at DDL time.

-- Table-level TTL with top-level AggregateFunction column
CREATE TABLE test_ttl_agg
(
    key1 String,
    key2 String,
    ts AggregateFunction(max, DateTime64(3))
)
ENGINE = MergeTree()
ORDER BY (key1, key2)
TTL toDateTime(ts) + INTERVAL 1 DAY; -- { serverError BAD_TTL_EXPRESSION }

-- Column-level TTL with AggregateFunction column
CREATE TABLE test_ttl_agg_col
(
    key1 String,
    key2 String,
    ts AggregateFunction(max, DateTime64(3)) TTL toDateTime(ts) + INTERVAL 1 DAY
)
ENGINE = MergeTree()
ORDER BY (key1, key2); -- { serverError BAD_TTL_EXPRESSION }

-- TTL DELETE WHERE referencing AggregateFunction column
CREATE TABLE test_ttl_agg_where
(
    key1 String,
    key2 String,
    d DateTime,
    ts AggregateFunction(max, DateTime64(3))
)
ENGINE = MergeTree()
ORDER BY (key1, key2)
TTL d + INTERVAL 1 DAY DELETE WHERE toDateTime(ts) > toDateTime(0); -- { serverError BAD_TTL_EXPRESSION }

-- Nested AggregateFunction inside Tuple
CREATE TABLE test_ttl_agg_nested
(
    key1 String,
    key2 String,
    ts Tuple(a UInt64, b AggregateFunction(max, DateTime64(3)))
)
ENGINE = MergeTree()
ORDER BY (key1, key2)
TTL toDateTime(ts.b) + INTERVAL 1 DAY; -- { serverError BAD_TTL_EXPRESSION }
