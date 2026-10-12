-- Tags: no-parallel-replicas

SET explain_query_plan_default = 'legacy';

-- The stateless-test server config sets `max_rows_to_read` / `max_rows_to_read_leaf`, and with a row limit in
-- effect an accepted read-in-order redoes range analysis on the parts the projection optimizer already selected,
-- so the reported part and granule totals would be relative to that narrowed set. Pin the limits off to keep the
-- plan counts below independent of the server configuration.
SET max_rows_to_read = 0, max_rows_to_read_leaf = 0;

CREATE TABLE test_proj_minmax
(
    a UInt32,
    b UInt32,
    PROJECTION p (SELECT a, b ORDER BY b) WITH SETTINGS (add_minmax_index_for_numeric_columns = 1)
)
ENGINE = MergeTree
PARTITION BY a % 3
ORDER BY a
SETTINGS index_granularity = 1, storage_policy = 'default';

INSERT INTO test_proj_minmax SELECT number, number FROM numbers(100);

SET force_optimize_projection = 1;
SET optimize_use_projections = 1;
SET enable_analyzer = 1;

EXPLAIN indexes = 1, projections = 1
SELECT a, b
FROM test_proj_minmax
WHERE b = 10
ORDER BY b;
