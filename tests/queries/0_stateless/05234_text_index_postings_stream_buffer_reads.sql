-- Tags: no-object-storage
-- The check below counts reads of local files; on object storage the posting list is not read through a file
-- descriptor at all.

-- Posting lists are read through streams whose buffer is sized to the segments they read. This test pins the
-- effect: a lazy read of a posting list of a few hundred KiB takes a handful of read syscalls instead of one per
-- 16 KiB. The results of the paths that do those reads are pinned by `05233_text_index_postings_stream_buffer`.

SET enable_full_text_index = 1;
SET use_skip_indexes = 1;
SET use_skip_indexes_on_data_read = 1;
SET query_plan_direct_read_from_text_index = 1;
SET use_query_condition_cache = 0;
SET use_text_index_postings_cache = 0;
SET merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0;
SET max_threads = 1;

DROP TABLE IF EXISTS t_postings_buffer_large;

-- One part and a posting_list_block_size above the cardinality (pinned, the table setting is randomized in
-- tests): `common` is a single segment of about 300 KiB, its packed deltas taking four to five bits because it
-- lands on every other row at random. A lazy cursor reads the segment whole: one or two reads with a buffer
-- sized to it, one per 16 KiB with the dictionary buffer.
CREATE TABLE t_postings_buffer_large
(
    id UInt64,
    s String,
    INDEX idx s TYPE text(tokenizer = 'splitByNonAlpha', posting_list_codec = 'bitpacking', posting_list_block_size = 1048576)
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 8192, index_granularity_bytes = '10M', min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_postings_buffer_large SELECT number, if(cityHash64(number) % 2 = 0, 'common other', 'other') FROM numbers(1000000);

SELECT count() = (SELECT countIf(cityHash64(number) % 2 = 0) FROM numbers(1000000)) FROM t_postings_buffer_large WHERE hasToken(s, 'common')
SETTINGS text_index_posting_list_apply_mode = 'lazy', query_plan_optimize_count_from_text_index = 0,
         local_filesystem_read_method = 'pread', use_page_cache_for_local_disks = 0, min_bytes_to_use_direct_io = 0,
         max_read_buffer_size_local_fs = 131072, log_comment = '05234_lazy_large_list';

SYSTEM FLUSH LOGS query_log;

-- Under parallel replicas the reads land on the replica rows, so take the largest count over the rows of the
-- query. Besides the posting list, the query reads the index header, a dictionary block, the marks and the
-- primary key, six reads in all. The posting list itself takes two reads through a buffer capped at the regular
-- local read buffer size of 128 KiB (`max_read_buffer_size_local_fs`) and fifteen through a 16 KiB one:
-- 23 reads in total with a buffer of 16 KiB, 8 with one sized to the segment.
SELECT max(ProfileEvents['ReadBufferFromFileDescriptorRead']) < 12
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
  AND type = 'QueryFinish'
  AND initial_query_id IN
  (
      SELECT query_id FROM system.query_log
      WHERE event_date >= yesterday() AND event_time >= now() - 600
        AND current_database = currentDatabase()
        AND type = 'QueryFinish'
        AND is_initial_query = 1
        AND log_comment = '05234_lazy_large_list'
  );

DROP TABLE t_postings_buffer_large;
