-- An aggregate is resolved on its argument types with LowCardinality removed recursively, so a column
-- that TTL GROUP BY aggregates implicitly (neither a GROUP BY key nor a SET target) comes back from the
-- aggregation without the nested LowCardinality the table declares. One column per nesting carrier.

DROP TABLE IF EXISTS t_ttl_group_by_nested_lc;

CREATE TABLE t_ttl_group_by_nested_lc
(
    k UInt32,
    ts DateTime('UTC'),
    tup Tuple(a LowCardinality(String), b UInt32),
    arr Array(LowCardinality(String)),
    kv Map(LowCardinality(String), UInt32)
)
ENGINE = MergeTree ORDER BY k
TTL ts + INTERVAL 1 DAY GROUP BY k SET ts = max(ts);

INSERT INTO t_ttl_group_by_nested_lc VALUES (1, '2020-01-01 00:00:00', ('x', 1), ['x', 'y'], map('x', 1)), (1, '2020-01-02 00:00:00', ('x', 1), ['x', 'y'], map('x', 1));
INSERT INTO t_ttl_group_by_nested_lc VALUES (2, '2020-01-03 00:00:00', ('z', 2), ['z'], map('z', 2)), (2, '2020-01-04 00:00:00', ('z', 2), ['z'], map('z', 2));

OPTIMIZE TABLE t_ttl_group_by_nested_lc FINAL;

SELECT k, ts, tup, arr, kv FROM t_ttl_group_by_nested_lc ORDER BY k;

DROP TABLE t_ttl_group_by_nested_lc;
