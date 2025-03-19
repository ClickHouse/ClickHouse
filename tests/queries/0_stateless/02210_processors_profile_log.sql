EXPLAIN PIPELINE SELECT sleep(1);

SELECT sleep(1) SETTINGS log_processors_profiles=true, log_queries=1, log_queries_min_type='QUERY_FINISH';
SYSTEM FLUSH LOGS;

WITH
    (
        SELECT query_id
        FROM system.query_log
        WHERE current_database = currentDatabase() AND Settings['log_processors_profiles']='1'
    ) AS query_id_
SELECT
    name,
    multiIf(
        -- ExpressionTransform executes sleep(),
        -- so IProcessor::work() will spend 1 sec.
        -- We use two different timers to measure time: CLOCK_MONOTONIC for sleep and CLOCK_MONOTONIC_COARSE for profiling
        -- that's why we cannot compare directly with 1,000,000 microseconds - let's compare with 900,000 microseconds.
        name = 'ExpressionTransform', elapsed_us >= 0.9e6 ? 1 : elapsed_us,
        -- SourceFromSingleChunk, that feed data to ExpressionTransform,
        -- will feed first block and then wait in PortFull.
        name = 'SourceFromSingleChunk', output_wait_elapsed_us >= 0.9e6 ? 1 : output_wait_elapsed_us,
        -- LazyOutputFormatLazyOutputFormat is the output
        -- so it cannot starts to execute before sleep(1) will be executed.
        input_wait_elapsed_us>=1e6 ? 1 : input_wait_elapsed_us)
    elapsed,
    input_rows,
    input_bytes,
    output_rows,
    output_bytes
FROM system.processors_profile_log
WHERE query_id = query_id_
ORDER BY name;
