-- Tags: no-parallel, no-sanitizers, no-debug, no-object-storage, no-fasttest, no-replicated-database
-- - no-parallel - too heavy (~10 GiB of RAM)

-- Test for crashes in jemalloc with LTO
-- https://github.com/ClickHouse/ClickHouse/issues/102460

SET async_socket_for_remote=1;
SET max_memory_usage='10Gi';
SET max_rows_to_transfer=0;
SET max_bytes_to_transfer=0;
SET max_result_rows=0;
SET max_result_bytes=0;

SET query_profiler_real_time_period_ns=0;
SET query_profiler_cpu_time_period_ns=0;
SET memory_profiler_step=0;
SET memory_profiler_sample_probability=0;

SELECT finalizeAggregation(summed) AS res
FROM
(
    SELECT
        key,
        sumMapState(CAST(range(rg), 'Array(UInt64)'), arrayWithConstant(rg, CAST('123', 'UInt64'))) AS summed
    FROM cluster(test_cluster_interserver_secret, view(
        SELECT
            number AS key,
            10 AS rg
        FROM numbers(4000000)
    ))
    GROUP BY key
    SETTINGS max_threads=8, distributed_group_by_no_merge=1, prefer_localhost_replica=0
)
FORMAT `Null`
