-- Keep effective `max_threads` as set below: under memory pressure
-- `getMaxThreadsForAvailableMemory` clamps it to 1, the `max_streams > 1` guard then skips
-- `max_streams_to_max_threads_ratio` entirely, and the truncating product is never formed.
SET max_threads_min_free_memory_per_thread = 0;

DROP TABLE IF EXISTS t_05037;
CREATE TABLE t_05037 (a UInt64) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_05037 SELECT number FROM numbers(100000);

-- A direct read is floored while the query is planned, so it must be unaffected at every ratio.
SELECT a FROM t_05037 ORDER BY a DESC LIMIT 3
SETTINGS max_threads = 2, max_streams_to_max_threads_ratio = 2147483648, optimize_read_in_order = 1;
SELECT sum(a) FROM t_05037
SETTINGS max_threads = 2, max_streams_to_max_threads_ratio = 2147483648;

-- A `merge()` read at a ratio that leaves the stream count intact must also be unaffected.
SELECT a FROM merge(currentDatabase(), '^t_05037$') ORDER BY a DESC LIMIT 3
SETTINGS max_threads = 2, max_streams_to_max_threads_ratio = 1073741824, optimize_read_in_order = 1;
SELECT sum(a) FROM merge(currentDatabase(), '^t_05037$')
SETTINGS max_threads = 2, max_streams_to_max_threads_ratio = 1073741824;

-- A healthy request must still be spread over more than one stream: the results above hold whether
-- or not the count survives, so without this nothing observes the count itself. The stream count is
-- reduced again by how much data each stream would get, so the granularity and both concurrent-read
-- minimums are pinned to keep enough marks to spread.
DROP TABLE IF EXISTS t_wide_05037;
CREATE TABLE t_wide_05037 (a UInt64) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 1024;
INSERT INTO t_wide_05037 SELECT number FROM numbers(100000);

SELECT count() > 0 FROM (EXPLAIN PIPELINE SELECT sum(a) FROM merge(currentDatabase(), '^t_wide_05037$')
SETTINGS max_threads = 4, max_streams_to_max_threads_ratio = 1,
         merge_tree_min_rows_for_concurrent_read = 0, merge_tree_min_bytes_for_concurrent_read = 0)
WHERE explain ILIKE '%Transform × %';

DROP TABLE t_wide_05037;
DROP TABLE t_05037;
