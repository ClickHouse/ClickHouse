-- Tags: no-parallel-replicas, no-darwin
-- no-darwin -- there is no preadv2 on Darwin, so the default local_filesystem_read_method is switched
--   from 'pread_threadpool' to 'pread' there, and asynchronous_read_counters stays empty
-- The Settings and asynchronous_read_counters columns of system.query_log are written by
-- QueryLogElement::appendToBlock straight into the Map subcolumns. This asserts they still
-- serialize with the right contents and row alignment.

DROP TABLE IF EXISTS t_query_log_maps;

CREATE TABLE t_query_log_maps (k UInt64, s String) ENGINE = MergeTree ORDER BY k
    SETTINGS index_granularity = 128, min_bytes_for_wide_part = 0;

INSERT INTO t_query_log_maps SELECT number, repeat('x', 200) FROM numbers(200000);

-- Settings: a numeric value, a string value and a custom setting.
SET max_block_size = 65413;
SET date_time_output_format = 'iso';
SET SQL_query_log_maps_probe = 'probe_value';

-- asynchronous_read_counters is only non-empty when the prefetched read pool runs, so the three
-- settings that decide that are pinned (both the local and the remote variant, so the test also
-- works on object-storage runs where the parts are not local):
--   * the read method must be pread_threadpool / threadpool (MergeTreePrefetchedReadPool::checkReadMethodAllowed);
--   * allow_prefetched_read_pool_for_* must be on;
--   * the PartsSplitter fault injection must be off - it takes precedence in ReadFromMergeTree and
--     reads ReadType::InOrder, which uses no prefetch pool at all.
SELECT sum(k) FROM t_query_log_maps
SETTINGS log_comment = '04640_settings_probe',
         merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0,
         local_filesystem_read_method = 'pread_threadpool',
         remote_filesystem_read_method = 'threadpool',
         allow_prefetched_read_pool_for_local_filesystem = 1,
         allow_prefetched_read_pool_for_remote_filesystem = 1,
         filesystem_prefetch_step_marks = 1,
         filesystem_prefetches_limit = 100,
         merge_tree_min_rows_for_concurrent_read = 1,
         merge_tree_min_bytes_for_concurrent_read = 1,
         max_threads = 4;

SYSTEM FLUSH LOGS query_log;

-- Exactly one QueryFinish row: a wrong Map offset would misalign every later column.
SELECT count() FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
  AND current_database = currentDatabase() AND type = 'QueryFinish'
  AND log_comment = '04640_settings_probe';

-- Settings keys and values round-trip, including the custom setting.
SELECT
    Settings['max_block_size'],
    Settings['date_time_output_format'],
    Settings['SQL_query_log_maps_probe'],
    Settings['log_comment'],
    has(mapKeys(Settings), 'max_threads')
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
  AND current_database = currentDatabase() AND type = 'QueryFinish'
  AND log_comment = '04640_settings_probe';

-- asynchronous_read_counters is written by the second changed hunk. The prefetched read pool
-- above makes it non-empty; assert the key set, not the (load-dependent) counter values.
SELECT arraySort(mapKeys(asynchronous_read_counters))
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
  AND current_database = currentDatabase() AND type = 'QueryFinish'
  AND log_comment = '04640_settings_probe';

-- ProfileEvents is written by the unchanged sibling site right before Settings: this guards
-- against a shifted columns[i++] index.
SELECT ProfileEvents['SelectedRows'] = 200000, length(mapKeys(ProfileEvents)) > 10
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
  AND current_database = currentDatabase() AND type = 'QueryFinish'
  AND log_comment = '04640_settings_probe';

DROP TABLE t_query_log_maps;
