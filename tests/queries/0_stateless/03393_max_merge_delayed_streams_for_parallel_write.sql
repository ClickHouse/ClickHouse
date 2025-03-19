-- Tags: no-fasttest, long, no-parallel, no-flaky-check
-- - no-fasttest -- S3 is required
-- - no-flaky-check -- not compatible with ThreadFuzzer

-- The real example with metric_log with 1200+ columns!
system flush logs system.metric_log;

create table metric_log as system.metric_log
engine = MergeTree
partition by ()
order by ()
settings
    storage_policy = 's3_no_cache',
    min_rows_for_wide_part = 0,
    min_bytes_for_wide_part = 0,
    vertical_merge_algorithm_min_rows_to_activate = 0,
    max_merge_delayed_streams_for_parallel_write = 1,
    -- avoid superfluous merges
    merge_selector_base = 1000
;

insert into metric_log select * from generateRandom() limit 10;

optimize table metric_log final;
system flush logs part_log;
select 'max_merge_delayed_streams_for_parallel_write=1' as test, * from system.part_log where table = 'metric_log' and database = currentDatabase() and event_date >= yesterday() and event_type = 'MergeParts' and peak_memory_usage > 100_000_000 format Vertical;

alter table metric_log modify setting max_merge_delayed_streams_for_parallel_write = 10000;

optimize table metric_log final;
system flush logs part_log;
select 'max_merge_delayed_streams_for_parallel_write=1000' as test, count() as count from system.part_log where table = 'metric_log' and database = currentDatabase() and event_date >= yesterday() and event_type = 'MergeParts' and peak_memory_usage > 1_000_000_000;
