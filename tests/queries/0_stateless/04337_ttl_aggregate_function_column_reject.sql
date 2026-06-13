-- Verify that CREATE TABLE rejects TTL expressions referencing AggregateFunction columns at DDL time.

-- Table-level TTL: toDateTime cannot accept AggregateFunction state
CREATE TABLE test_ttl_agg
(
    key1 String,
    key2 String,
    ts AggregateFunction(max, DateTime64(3))
)
ENGINE = MergeTree()
ORDER BY (key1, key2)
TTL toDateTime(ts) + INTERVAL 1 DAY; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- Column-level TTL: same issue
CREATE TABLE test_ttl_agg_col
(
    key1 String,
    key2 String,
    ts AggregateFunction(max, DateTime64(3)) TTL toDateTime(ts) + INTERVAL 1 DAY
)
ENGINE = MergeTree()
ORDER BY (key1, key2); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- TTL DELETE WHERE: toDateTime on AggregateFunction in WHERE clause
CREATE TABLE test_ttl_agg_where
(
    key1 String,
    key2 String,
    d DateTime,
    ts AggregateFunction(max, DateTime64(3))
)
ENGINE = MergeTree()
ORDER BY (key1, key2)
TTL d + INTERVAL 1 DAY DELETE WHERE toDateTime(ts) > toDateTime(0); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- Valid usage: finalizeAggregation can operate on AggregateFunction states
CREATE TABLE test_ttl_agg_finalize
(
    key1 String,
    key2 String,
    ts AggregateFunction(max, DateTime64(3))
)
ENGINE = MergeTree()
ORDER BY (key1, key2)
TTL toDateTime(finalizeAggregation(ts)) + INTERVAL 1 DAY;

DROP TABLE test_ttl_agg_finalize;
