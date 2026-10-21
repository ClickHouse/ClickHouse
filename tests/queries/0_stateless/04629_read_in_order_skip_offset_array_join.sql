-- Tags: no-random-merge-tree-settings

-- A projection-list arrayJoin expands rows after they leave the read step, so OFFSET counts post-expansion
-- rows. The OFFSET-skip read-in-order optimization walks through Expression steps, but must bail out when the
-- expression contains an arrayJoin, since it would otherwise trim source granules by pre-expansion row counts.

DROP TABLE IF EXISTS t_skip_offset_array_join;
CREATE TABLE t_skip_offset_array_join (k UInt64, arr Array(UInt64)) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 1;
INSERT INTO t_skip_offset_array_join VALUES (1, [10, 11]), (2, [20]);

SELECT arrayJoin(arr) FROM t_skip_offset_array_join ORDER BY k LIMIT 1 OFFSET 1
SETTINGS optimize_read_in_order = 1, query_plan_optimize_read_in_order_skip_offset = 1;

DROP TABLE t_skip_offset_array_join;
