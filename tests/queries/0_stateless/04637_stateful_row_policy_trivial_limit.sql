-- Regression test: the trivial-`LIMIT` fast path must force a single deterministic read stream when
-- a stateful function (e.g. `neighbor`, `logTrace`) sits in a hidden reader-side filter - a row
-- policy or `additional_table_filters` - and not only when it sits in the SELECT list. Such a filter
-- is evaluated on the read side, so splitting the read across streams (or across parallel replicas)
-- changes the rows and blocks the stateful function observes.
-- A deterministic hidden filter keeps the multi-stream read: only the source cap is suppressed there.

DROP TABLE IF EXISTS t_stateful_policy;
DROP TABLE IF EXISTS t_plain_policy;

CREATE TABLE t_stateful_policy (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 8;
INSERT INTO t_stateful_policy SELECT number, 20 FROM numbers(1000);

CREATE TABLE t_plain_policy (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 8;
INSERT INTO t_plain_policy SELECT number, 20 FROM numbers(1000);

CREATE ROW POLICY rp_stateful ON t_stateful_policy USING neighbor(v, 1) = 20 TO ALL;
CREATE ROW POLICY rp_plain ON t_plain_policy USING v = 20 TO ALL;

SET max_threads = 4;
SET max_block_size = 65536;
SET merge_tree_min_rows_for_concurrent_read = 1;
SET merge_tree_min_bytes_for_concurrent_read = 1;
SET enable_parallel_replicas = 0;
SET allow_deprecated_error_prone_window_functions = 1;

SET enable_analyzer = 1;

-- A stateful row policy - a single stream.
SELECT 'analyzer, policy stateful', if(toUInt64OrDefault(extract(explain, '× (\d+)'), 1::UInt64) = 1, 'single stream', 'multiple streams')
FROM (EXPLAIN PIPELINE SELECT k FROM t_stateful_policy LIMIT 1000)
WHERE explain LIKE '%MergeTreeSelect%';

-- A deterministic row policy - the read still uses several streams.
SELECT 'analyzer, policy plain', if(toUInt64OrDefault(extract(explain, '× (\d+)'), 1::UInt64) = 1, 'single stream', 'multiple streams')
FROM (EXPLAIN PIPELINE SELECT k FROM t_plain_policy LIMIT 1000)
WHERE explain LIKE '%MergeTreeSelect%';

-- The same for `additional_table_filters`.
SELECT 'analyzer, additional filter stateful', if(toUInt64OrDefault(extract(explain, '× (\d+)'), 1::UInt64) = 1, 'single stream', 'multiple streams')
FROM (EXPLAIN PIPELINE SELECT k FROM t_plain_policy LIMIT 1000
      SETTINGS additional_table_filters = {'t_plain_policy': 'neighbor(v, 1) = 20'})
WHERE explain LIKE '%MergeTreeSelect%';

SELECT 'analyzer, additional filter plain', if(toUInt64OrDefault(extract(explain, '× (\d+)'), 1::UInt64) = 1, 'single stream', 'multiple streams')
FROM (EXPLAIN PIPELINE SELECT k FROM t_plain_policy LIMIT 1000
      SETTINGS additional_table_filters = {'t_plain_policy': 'v = 20'})
WHERE explain LIKE '%MergeTreeSelect%';

SET enable_analyzer = 0;

SELECT 'old analyzer, policy stateful', if(toUInt64OrDefault(extract(explain, '× (\d+)'), 1::UInt64) = 1, 'single stream', 'multiple streams')
FROM (EXPLAIN PIPELINE SELECT k FROM t_stateful_policy LIMIT 1000)
WHERE explain LIKE '%MergeTreeSelect%';

SELECT 'old analyzer, policy plain', if(toUInt64OrDefault(extract(explain, '× (\d+)'), 1::UInt64) = 1, 'single stream', 'multiple streams')
FROM (EXPLAIN PIPELINE SELECT k FROM t_plain_policy LIMIT 1000)
WHERE explain LIKE '%MergeTreeSelect%';

SELECT 'old analyzer, additional filter stateful', if(toUInt64OrDefault(extract(explain, '× (\d+)'), 1::UInt64) = 1, 'single stream', 'multiple streams')
FROM (EXPLAIN PIPELINE SELECT k FROM t_plain_policy LIMIT 1000
      SETTINGS additional_table_filters = {'t_plain_policy': 'neighbor(v, 1) = 20'})
WHERE explain LIKE '%MergeTreeSelect%';

SELECT 'old analyzer, additional filter plain', if(toUInt64OrDefault(extract(explain, '× (\d+)'), 1::UInt64) = 1, 'single stream', 'multiple streams')
FROM (EXPLAIN PIPELINE SELECT k FROM t_plain_policy LIMIT 1000
      SETTINGS additional_table_filters = {'t_plain_policy': 'v = 20'})
WHERE explain LIKE '%MergeTreeSelect%';

SET enable_analyzer = 1;

DROP ROW POLICY rp_stateful ON t_stateful_policy;
DROP ROW POLICY rp_plain ON t_plain_policy;
DROP TABLE t_stateful_policy;
DROP TABLE t_plain_policy;
