-- Tags: no-random-settings, no-random-merge-tree-settings
-- Regression test for `optimize_aggregation_in_order = 1` on a query without a WHERE
-- clause: every granule is selected, so PK "selectivity" is 100% and would naively
-- trip the `read_in_order_max_primary_key_ratio` guard. But the aggregation-in-order
-- path uses read-in-order to enable a STREAMING aggregation algorithm (memory bound),
-- not to skip a separate sort, so the guard must not fire here. If it did, the
-- algorithm would silently fall back to batched aggregation and the query below
-- could exceed the configured memory limit.
--
-- Each query sets `enable_parallel_replicas = 0` because the coordinator is keyed by
-- table name and persists across stress-test queries. A concurrent `Default`-mode
-- query against the same table would otherwise cause `Coordination mode mismatch`
-- when this test's `WithOrder` query announces its ranges.

DROP TABLE IF EXISTS t_aio_pk;

CREATE TABLE t_aio_pk (key UInt64) ENGINE = MergeTree ORDER BY key
SETTINGS index_granularity = 1024, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;

INSERT INTO t_aio_pk SELECT number FROM numbers(1000000);

-- Aggregation-in-order on the full table must still stream blocks to honor
-- the tight memory limit. Reducing `read_in_order_max_primary_key_ratio` to a
-- value below the actual selectivity (which is 1.0 here, no WHERE clause)
-- previously disabled read-in-order and caused MEMORY_LIMIT_EXCEEDED.
SELECT count() FROM (
    SELECT key FROM t_aio_pk GROUP BY key ORDER BY key LIMIT 100
    SETTINGS enable_parallel_replicas = 0,
             optimize_aggregation_in_order = 1,
             max_memory_usage = '50Mi',
             read_in_order_max_primary_key_ratio = 0.1
);

-- Same for `DISTINCT`-in-order: streaming `DISTINCT` must remain active.
SELECT count() FROM (
    SELECT DISTINCT key FROM t_aio_pk ORDER BY key LIMIT 100
    SETTINGS enable_parallel_replicas = 0,
             optimize_distinct_in_order = 1,
             max_memory_usage = '50Mi',
             read_in_order_max_primary_key_ratio = 0.1
);

DROP TABLE t_aio_pk;
