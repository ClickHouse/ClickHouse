-- Tags: no-random-merge-tree-settings, no-parallel-replicas

-- `optimizeReadInOrder` keeps an in-order read for a monotonic transformation of the sorting key, so here the
-- streams are merged on `toDate(ts)` while the primary index holds raw `ts`. Granules strictly separated on
-- `ts` can still be tied on `toDate(ts)`, so the OFFSET-skip optimization must not drop them: its output has
-- to match the unoptimized plan's.

DROP TABLE IF EXISTS t_skip_offset_monotonic;
DROP TABLE IF EXISTS t_skip_offset_monotonic_results;

CREATE TABLE t_skip_offset_monotonic (ts DateTime, v UInt64) ENGINE = MergeTree ORDER BY ts SETTINGS index_granularity = 2;
SYSTEM STOP MERGES t_skip_offset_monotonic;

INSERT INTO t_skip_offset_monotonic VALUES ('2020-01-01 00:00:00', 1), ('2020-01-01 00:00:01', 2), ('2020-01-01 00:00:02', 3), ('2020-01-01 00:00:03', 4);
INSERT INTO t_skip_offset_monotonic VALUES ('2020-01-01 06:00:00', 5), ('2020-01-01 06:00:01', 6), ('2020-01-02 00:00:00', 7), ('2020-01-02 00:00:01', 8);

CREATE TABLE t_skip_offset_monotonic_results (offset UInt64, rows String) ENGINE = Memory;

-- `query_plan_optimize_read_in_order_skip_offset` only takes effect at statement level: query-plan
-- optimizations run once for the whole plan, so a SETTINGS clause on a subquery is ignored.
SET optimize_read_in_order = 1, max_threads = 1;

SET query_plan_optimize_read_in_order_skip_offset = 0;
INSERT INTO t_skip_offset_monotonic_results SELECT 2, toString(groupArray(v)) FROM (SELECT v FROM t_skip_offset_monotonic ORDER BY toDate(ts) LIMIT 4 OFFSET 2);
INSERT INTO t_skip_offset_monotonic_results SELECT 4, toString(groupArray(v)) FROM (SELECT v FROM t_skip_offset_monotonic ORDER BY toDate(ts) LIMIT 4 OFFSET 4);

SET query_plan_optimize_read_in_order_skip_offset = 1;
INSERT INTO t_skip_offset_monotonic_results SELECT 2, toString(groupArray(v)) FROM (SELECT v FROM t_skip_offset_monotonic ORDER BY toDate(ts) LIMIT 4 OFFSET 2);
INSERT INTO t_skip_offset_monotonic_results SELECT 4, toString(groupArray(v)) FROM (SELECT v FROM t_skip_offset_monotonic ORDER BY toDate(ts) LIMIT 4 OFFSET 4);

SELECT concat('offset ', toString(offset)), if(uniqExact(rows) = 1, 'ok', 'FAIL')
FROM t_skip_offset_monotonic_results GROUP BY offset ORDER BY offset;

DROP TABLE t_skip_offset_monotonic_results;
DROP TABLE t_skip_offset_monotonic;
