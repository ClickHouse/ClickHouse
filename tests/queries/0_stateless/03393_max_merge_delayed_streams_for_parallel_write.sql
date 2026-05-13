-- Tags: no-fasttest, long, no-parallel, no-flaky-check, no-msan, no-tsan
-- - no-fasttest -- S3 is required
-- - no-flaky-check -- not compatible with ThreadFuzzer
-- - no-tsan -- merging 1200+ columns on `s3_no_cache` is intrinsically slow under TSan and
--              consistently exceeds the 300 s client `receive_timeout` on `OPTIMIZE FINAL`.
--              The merge itself is healthy; only the client wait window is the issue, and
--              the global per-test timeout (10 min) makes bumping it impractical.

-- The real example with metric_log with 1200+ columns!
SET optimize_trivial_insert_select = 0;
-- Since #102961 ("Use max_insert_threads for plain INSERTs without materialized
-- views"), the INSERT below fans out into `max_insert_threads` parallel writer
-- threads. Each thread opens its own copy of all column streams and each stream
-- pre-allocates `max_compress_block_size` + `DBMS_DEFAULT_BUFFER_SIZE`, so the
-- writer footprint scales as N_threads * N_columns * ~2 MiB. With 1200+ columns
-- and the randomized `max_insert_threads = 3`, the INSERT can exceed the default
-- 4.66 GiB per-query memory limit on regular (non-sanitizer) builds as well —
-- see https://s3.amazonaws.com/clickhouse-test-reports/json.html?PR=76867&sha=8175ad6ae6252d416ba993505609e4fb0407c582&name_0=PR&name_1=Stateless%20tests%20%28arm_binary%2C%20sequential%29.
-- Pin to a single writer thread; the test verifies merge behaviour, not parallel
-- insert performance.
SET max_insert_threads = 1;

system flush logs system.metric_log;

create table metric_log as system.metric_log
engine = MergeTree
partition by ()
order by ()
settings
    -- cache has it's own problems (see filesystem_cache_prefer_bigger_buffer_size)
    storage_policy = 's3_no_cache',
    -- horizontal merges does opens all stream at once, so will still use huge amount of memory
    min_rows_for_wide_part = 0,
    min_bytes_for_wide_part = 0,
    vertical_merge_algorithm_min_rows_to_activate = 0,
    vertical_merge_algorithm_min_columns_to_activate = 1,
    min_bytes_for_full_part_storage = 0,
    --- avoid excessive memory usage (due to default buffer size of 1MiB that is created for each column)
    max_merge_delayed_streams_for_parallel_write = 100,
    -- avoid superfluous merges
    merge_selector_base = 1000,
    min_columns_to_activate_adaptive_write_buffer = 0,
    auto_statistics_types = '';

insert into metric_log select * from generateRandom() limit 10;

optimize table metric_log final;
system flush logs part_log;
select 'max_merge_delayed_streams_for_parallel_write=100' as test, * from system.part_log where table = 'metric_log' and database = currentDatabase() and event_date >= yesterday() AND event_time >= now() - 600 and event_type = 'MergeParts' and peak_memory_usage > 1_000_000_000 format Vertical;

alter table metric_log modify setting max_merge_delayed_streams_for_parallel_write = 10000;

optimize table metric_log final;
system flush logs part_log;
select 'max_merge_delayed_streams_for_parallel_write=1000' as test, count() as count from system.part_log where table = 'metric_log' and database = currentDatabase() and event_date >= yesterday() AND event_time >= now() - 600 and event_type = 'MergeParts' and peak_memory_usage > 1_000_000_000;
