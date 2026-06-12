-- Verify that CREATE TABLE rejects TTL expressions referencing AggregateFunction columns at DDL time.

CREATE TABLE test_ttl_agg
(
    key1 String,
    key2 String,
    ts AggregateFunction(max, DateTime64(3))
)
ENGINE = MergeTree()
ORDER BY (key1, key2)
TTL toDateTime(ts) + INTERVAL 1 DAY; -- { serverError BAD_TTL_EXPRESSION }

CREATE TABLE test_ttl_agg_col
(
    key1 String,
    key2 String,
    ts AggregateFunction(max, DateTime64(3)) TTL toDateTime(ts) + INTERVAL 1 DAY
)
ENGINE = MergeTree()
ORDER BY (key1, key2); -- { serverError BAD_TTL_EXPRESSION }
