-- Tags: long, no-tsan, no-msan, no-distributed-cache
-- Too long for TSan and MSan

set enable_filesystem_cache=0;
set enable_filesystem_cache_on_write_operations=0;
set max_rows_to_read = '30M';

drop table if exists t;

create table t (x UInt64, s String) engine = MergeTree order by x SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';
INSERT INTO t SELECT
    number,
    if(number < (8129 * 1024), arrayStringConcat(arrayMap(x -> toString(x), range(number % 128)), ' '), '')
FROM numbers_mt((8129 * 1024) * 3) settings max_insert_threads=8, max_rows_to_read=0, max_memory_usage='10Gi';

-- optimize table t final;

select count(), sum(length(s)) from t settings max_threads = 3, read_backoff_min_latency_ms = 1, read_backoff_max_throughput = 1000000000, read_backoff_min_interval_between_events_ms = 1, read_backoff_min_events = 1, read_backoff_min_concurrency = 1;
select count(), sum(length(s)) from t settings max_threads = 3, read_backoff_min_latency_ms = 1, read_backoff_max_throughput = 1000000000, read_backoff_min_interval_between_events_ms = 1, read_backoff_min_events = 1, read_backoff_min_concurrency = 1;
select count(), sum(length(s)) from t settings max_threads = 3, read_backoff_min_latency_ms = 1, read_backoff_max_throughput = 1000000000, read_backoff_min_interval_between_events_ms = 1, read_backoff_min_events = 1, read_backoff_min_concurrency = 1;

drop table if exists t;
