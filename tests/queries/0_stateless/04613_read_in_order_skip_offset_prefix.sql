-- Tags: no-random-merge-tree-settings, no-parallel-replicas

-- Read-in-order for `ORDER BY a` on a table sorted by (a, b) merges parts on `a` only, so granules that
-- are strictly separated on the full key (a, b) can still be tied in the merge order. The OFFSET-skip
-- optimization must stay result-preserving: its output must match the unoptimized plan's.

DROP TABLE IF EXISTS t_skip_offset_prefix;
CREATE TABLE t_skip_offset_prefix (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY (a, b) SETTINGS index_granularity = 4;
SYSTEM STOP MERGES t_skip_offset_prefix;

INSERT INTO t_skip_offset_prefix VALUES (0, 0), (0, 1), (0, 2), (0, 3);
INSERT INTO t_skip_offset_prefix VALUES (1, 0), (1, 2), (1, 4), (1, 6), (2, 0), (2, 2), (2, 4), (2, 6);
INSERT INTO t_skip_offset_prefix VALUES (1, 1), (1, 3), (1, 5), (1, 7), (2, 1), (2, 3), (2, 5), (2, 7);

SELECT 'offset 0', if(
    (SELECT groupArray((a, b)) FROM (SELECT a, b FROM t_skip_offset_prefix ORDER BY a LIMIT 4 OFFSET 0 SETTINGS query_plan_optimize_read_in_order_skip_offset = 1, max_threads = 1))
  = (SELECT groupArray((a, b)) FROM (SELECT a, b FROM t_skip_offset_prefix ORDER BY a LIMIT 4 OFFSET 0 SETTINGS query_plan_optimize_read_in_order_skip_offset = 0, max_threads = 1)),
  'ok', 'FAIL');

SELECT 'offset 4', if(
    (SELECT groupArray((a, b)) FROM (SELECT a, b FROM t_skip_offset_prefix ORDER BY a LIMIT 4 OFFSET 4 SETTINGS query_plan_optimize_read_in_order_skip_offset = 1, max_threads = 1))
  = (SELECT groupArray((a, b)) FROM (SELECT a, b FROM t_skip_offset_prefix ORDER BY a LIMIT 4 OFFSET 4 SETTINGS query_plan_optimize_read_in_order_skip_offset = 0, max_threads = 1)),
  'ok', 'FAIL');

SELECT 'offset 6', if(
    (SELECT groupArray((a, b)) FROM (SELECT a, b FROM t_skip_offset_prefix ORDER BY a LIMIT 4 OFFSET 6 SETTINGS query_plan_optimize_read_in_order_skip_offset = 1, max_threads = 1))
  = (SELECT groupArray((a, b)) FROM (SELECT a, b FROM t_skip_offset_prefix ORDER BY a LIMIT 4 OFFSET 6 SETTINGS query_plan_optimize_read_in_order_skip_offset = 0, max_threads = 1)),
  'ok', 'FAIL');

SELECT 'offset 8', if(
    (SELECT groupArray((a, b)) FROM (SELECT a, b FROM t_skip_offset_prefix ORDER BY a LIMIT 4 OFFSET 8 SETTINGS query_plan_optimize_read_in_order_skip_offset = 1, max_threads = 1))
  = (SELECT groupArray((a, b)) FROM (SELECT a, b FROM t_skip_offset_prefix ORDER BY a LIMIT 4 OFFSET 8 SETTINGS query_plan_optimize_read_in_order_skip_offset = 0, max_threads = 1)),
  'ok', 'FAIL');

SELECT 'offset 12', if(
    (SELECT groupArray((a, b)) FROM (SELECT a, b FROM t_skip_offset_prefix ORDER BY a LIMIT 4 OFFSET 12 SETTINGS query_plan_optimize_read_in_order_skip_offset = 1, max_threads = 1))
  = (SELECT groupArray((a, b)) FROM (SELECT a, b FROM t_skip_offset_prefix ORDER BY a LIMIT 4 OFFSET 12 SETTINGS query_plan_optimize_read_in_order_skip_offset = 0, max_threads = 1)),
  'ok', 'FAIL');

DROP TABLE t_skip_offset_prefix;
