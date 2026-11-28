-- Tags: no-object-storage
-- On remote disks without a filesystem cache, gaps of one granule are read through, which changes the granule counts.
-- `optimize_aggregation_max_by_sorting_key` over `remote`: every shard skips granules in its own read, and the
-- initiator merges the partial results. Both shards read the same table, which has 2 runs of 3 granules each.
-- Every query is run with the optimization off and on: the results must be equal, and with it on both shards
-- read fewer rows.

SET enable_parallel_replicas = 0;
-- The injection splits the read ranges with `PartsSplitter` after this optimization, which can read skipped granules
-- again: the result is the same, but the rows read are not.
SET merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0;
-- One part per `INSERT`, so that the number of granules is deterministic.
SET max_block_size = 65409, max_insert_block_size = 1048449, min_insert_block_size_rows = 1048449, min_insert_block_size_bytes = 0;

DROP TABLE IF EXISTS t;
CREATE TABLE t (k String, m UInt32, x UInt32) ENGINE = MergeTree ORDER BY (k, m)
    SETTINGS index_granularity = 4, index_granularity_bytes = '10Mi';
INSERT INTO t SELECT if(number < 12, 'A', 'B'), number % 12 + 1, 1000 - number FROM numbers(24);

SELECT '-- max and argMax';
SELECT k, max(m), argMax(x, m) FROM remote('127.0.0.{1,2}', currentDatabase(), t) GROUP BY k ORDER BY k
    SETTINGS optimize_aggregation_max_by_sorting_key = 0, log_comment = 'max_by_sorting_key_max_off';
SELECT k, max(m), argMax(x, m) FROM remote('127.0.0.{1,2}', currentDatabase(), t) GROUP BY k ORDER BY k
    SETTINGS optimize_aggregation_max_by_sorting_key = 1, log_comment = 'max_by_sorting_key_max_on';

SELECT '-- min and argMin';
SELECT k, min(m), argMin(x, m) FROM remote('127.0.0.{1,2}', currentDatabase(), t) GROUP BY k ORDER BY k
    SETTINGS optimize_aggregation_max_by_sorting_key = 0, log_comment = 'max_by_sorting_key_min_off';
SELECT k, min(m), argMin(x, m) FROM remote('127.0.0.{1,2}', currentDatabase(), t) GROUP BY k ORDER BY k
    SETTINGS optimize_aggregation_max_by_sorting_key = 1, log_comment = 'max_by_sorting_key_min_on';

SELECT '-- DISTINCT';
SELECT DISTINCT k FROM remote('127.0.0.{1,2}', currentDatabase(), t) ORDER BY k
    SETTINGS optimize_aggregation_max_by_sorting_key = 0, log_comment = 'max_by_sorting_key_distinct_off';
SELECT DISTINCT k FROM remote('127.0.0.{1,2}', currentDatabase(), t) ORDER BY k
    SETTINGS optimize_aggregation_max_by_sorting_key = 1, log_comment = 'max_by_sorting_key_distinct_on';

SELECT '-- LIMIT BY';
SELECT k, m, x FROM remote('127.0.0.{1,2}', currentDatabase(), t) ORDER BY k, m DESC LIMIT 2 BY k
    SETTINGS optimize_aggregation_max_by_sorting_key = 0, log_comment = 'max_by_sorting_key_limit_by_off';
SELECT k, m, x FROM remote('127.0.0.{1,2}', currentDatabase(), t) ORDER BY k, m DESC LIMIT 2 BY k
    SETTINGS optimize_aggregation_max_by_sorting_key = 1, log_comment = 'max_by_sorting_key_limit_by_on';

SELECT '-- filter on m';
SELECT k, argMax(x, m) FROM remote('127.0.0.{1,2}', currentDatabase(), t) WHERE m < 10 GROUP BY k ORDER BY k
    SETTINGS optimize_aggregation_max_by_sorting_key = 0, log_comment = 'max_by_sorting_key_filter_off';
SELECT k, argMax(x, m) FROM remote('127.0.0.{1,2}', currentDatabase(), t) WHERE m < 10 GROUP BY k ORDER BY k
    SETTINGS optimize_aggregation_max_by_sorting_key = 1, log_comment = 'max_by_sorting_key_filter_on';

SELECT '-- rows read by both shards';
SYSTEM FLUSH LOGS query_log;
SELECT log_comment, read_rows FROM system.query_log
WHERE event_date >= yesterday() AND current_database = currentDatabase() AND type = 'QueryFinish' AND is_initial_query
    AND startsWith(log_comment, 'max_by_sorting_key_')
ORDER BY log_comment;

DROP TABLE t;
