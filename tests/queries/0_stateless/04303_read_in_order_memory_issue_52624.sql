-- Tags: no-parallel-replicas, no-random-settings
-- no-parallel-replicas: we are using query_log to check the number of read rows
-- no-random-settings: the test verifies the default value of read_in_order_use_virtual_row,
--                     which the settings randomizer would otherwise override

-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/52624
--
-- When reading in order of the primary key over many parts, the merging pipeline
-- used to open a reader for every part at once, keeping a CompressedReadBuffer per
-- column resident for each of them. That made `ORDER BY pk LIMIT n` consume memory
-- proportional to the number of parts even though only a few of them are relevant.
--
-- `read_in_order_use_virtual_row` (enabled by default) lets MergingSortedTransform
-- reprioritize sources using primary key values from the sparse index, so the parts
-- that cannot contribute to the answer are never read. This test verifies that the
-- optimization is active by default: only a tiny fraction of the rows is read.

SET use_query_condition_cache = 0;
SET use_skip_indexes_for_top_k = 0;
SET use_top_k_dynamic_filtering = 0;
SET optimize_read_in_order = 1;
SET max_threads = 1;

DROP TABLE IF EXISTS t_52624;

CREATE TABLE t_52624 (k UInt64)
ENGINE = MergeTree ORDER BY k
SETTINGS index_granularity = 8192, add_minmax_index_for_numeric_columns = 0;

SYSTEM STOP MERGES t_52624;

-- 16 parts with disjoint, ascending key ranges: part i covers [i * 100000, i * 100000 + 16384).
INSERT INTO t_52624 SELECT number + 0  * 100000 FROM numbers(16384);
INSERT INTO t_52624 SELECT number + 1  * 100000 FROM numbers(16384);
INSERT INTO t_52624 SELECT number + 2  * 100000 FROM numbers(16384);
INSERT INTO t_52624 SELECT number + 3  * 100000 FROM numbers(16384);
INSERT INTO t_52624 SELECT number + 4  * 100000 FROM numbers(16384);
INSERT INTO t_52624 SELECT number + 5  * 100000 FROM numbers(16384);
INSERT INTO t_52624 SELECT number + 6  * 100000 FROM numbers(16384);
INSERT INTO t_52624 SELECT number + 7  * 100000 FROM numbers(16384);
INSERT INTO t_52624 SELECT number + 8  * 100000 FROM numbers(16384);
INSERT INTO t_52624 SELECT number + 9  * 100000 FROM numbers(16384);
INSERT INTO t_52624 SELECT number + 10 * 100000 FROM numbers(16384);
INSERT INTO t_52624 SELECT number + 11 * 100000 FROM numbers(16384);
INSERT INTO t_52624 SELECT number + 12 * 100000 FROM numbers(16384);
INSERT INTO t_52624 SELECT number + 13 * 100000 FROM numbers(16384);
INSERT INTO t_52624 SELECT number + 14 * 100000 FROM numbers(16384);
INSERT INTO t_52624 SELECT number + 15 * 100000 FROM numbers(16384);

SELECT count() AS total_rows, uniqExact(_part) AS parts FROM t_52624;

-- The smallest keys live in the first part only, so the answer is exact and cheap.
SELECT k FROM t_52624 ORDER BY k ASC LIMIT 5 SETTINGS log_comment = '04303_asc';
-- The largest keys live in the last part only.
SELECT k FROM t_52624 ORDER BY k DESC LIMIT 5 SETTINGS log_comment = '04303_desc';

SYSTEM FLUSH LOGS query_log;

-- Only a small number of rows must be read (a handful of granules), not all 16 parts.
-- Without the optimization every part would be opened and roughly all rows would be read.
SELECT read_rows < 100000
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
  AND current_database = currentDatabase()
  AND log_comment IN ('04303_asc', '04303_desc')
  AND type = 'QueryFinish'
ORDER BY log_comment;

DROP TABLE t_52624;
