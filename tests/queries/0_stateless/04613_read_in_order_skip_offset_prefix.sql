-- Tags: no-random-merge-tree-settings, no-parallel-replicas

-- Read-in-order for `ORDER BY a` on a table sorted by (a, b) merges parts on `a` only, so granules that
-- are strictly separated on the full key (a, b) can still be tied in the merge order. The OFFSET-skip
-- optimization must stay result-preserving: its output must match the unoptimized plan's.

DROP TABLE IF EXISTS t_skip_offset_prefix;
DROP TABLE IF EXISTS t_skip_offset_prefix_results;

CREATE TABLE t_skip_offset_prefix (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY (a, b) SETTINGS index_granularity = 4;
SYSTEM STOP MERGES t_skip_offset_prefix;

INSERT INTO t_skip_offset_prefix VALUES (0, 0), (0, 1), (0, 2), (0, 3);
INSERT INTO t_skip_offset_prefix VALUES (1, 0), (1, 2), (1, 4), (1, 6), (2, 0), (2, 2), (2, 4), (2, 6);
INSERT INTO t_skip_offset_prefix VALUES (1, 1), (1, 3), (1, 5), (1, 7), (2, 1), (2, 3), (2, 5), (2, 7);

CREATE TABLE t_skip_offset_prefix_results (offset UInt64, rows String) ENGINE = Memory;

-- `query_plan_optimize_read_in_order_skip_offset` only takes effect at statement level: query-plan
-- optimizations run once for the whole plan, so a SETTINGS clause on a subquery is ignored.
SET optimize_read_in_order = 1, max_threads = 1;

SET query_plan_optimize_read_in_order_skip_offset = 0;
INSERT INTO t_skip_offset_prefix_results SELECT 0, toString(groupArray((a, b))) FROM (SELECT a, b FROM t_skip_offset_prefix ORDER BY a LIMIT 4 OFFSET 0);
INSERT INTO t_skip_offset_prefix_results SELECT 4, toString(groupArray((a, b))) FROM (SELECT a, b FROM t_skip_offset_prefix ORDER BY a LIMIT 4 OFFSET 4);
INSERT INTO t_skip_offset_prefix_results SELECT 6, toString(groupArray((a, b))) FROM (SELECT a, b FROM t_skip_offset_prefix ORDER BY a LIMIT 4 OFFSET 6);
INSERT INTO t_skip_offset_prefix_results SELECT 8, toString(groupArray((a, b))) FROM (SELECT a, b FROM t_skip_offset_prefix ORDER BY a LIMIT 4 OFFSET 8);
INSERT INTO t_skip_offset_prefix_results SELECT 12, toString(groupArray((a, b))) FROM (SELECT a, b FROM t_skip_offset_prefix ORDER BY a LIMIT 4 OFFSET 12);

SET query_plan_optimize_read_in_order_skip_offset = 1;
INSERT INTO t_skip_offset_prefix_results SELECT 0, toString(groupArray((a, b))) FROM (SELECT a, b FROM t_skip_offset_prefix ORDER BY a LIMIT 4 OFFSET 0);
INSERT INTO t_skip_offset_prefix_results SELECT 4, toString(groupArray((a, b))) FROM (SELECT a, b FROM t_skip_offset_prefix ORDER BY a LIMIT 4 OFFSET 4);
INSERT INTO t_skip_offset_prefix_results SELECT 6, toString(groupArray((a, b))) FROM (SELECT a, b FROM t_skip_offset_prefix ORDER BY a LIMIT 4 OFFSET 6);
INSERT INTO t_skip_offset_prefix_results SELECT 8, toString(groupArray((a, b))) FROM (SELECT a, b FROM t_skip_offset_prefix ORDER BY a LIMIT 4 OFFSET 8);
INSERT INTO t_skip_offset_prefix_results SELECT 12, toString(groupArray((a, b))) FROM (SELECT a, b FROM t_skip_offset_prefix ORDER BY a LIMIT 4 OFFSET 12);

SELECT concat('offset ', toString(offset)), if(uniqExact(rows) = 1, 'ok', 'FAIL')
FROM t_skip_offset_prefix_results GROUP BY offset ORDER BY offset;

DROP TABLE t_skip_offset_prefix_results;
DROP TABLE t_skip_offset_prefix;
