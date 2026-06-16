-- Regression test for pipeline stuck with PingPongProcessor (ReadHeadBalancedProcessor)
-- deadlock when used in a full_sorting_merge join with FilterBySetOnTheFly optimization.
--
-- The deadlock occurred because Order::First blocked waiting for a ping from Order::Second
-- before processing any regular data. When MergeJoinTransform needed data from both sides,
-- the right side (Second) couldn't consume enough data to send the ping because the downstream
-- was blocked waiting for left-side data — a circular dependency.
--
-- https://github.com/ClickHouse/ClickHouse/issues/57728

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;

CREATE TABLE t1 (c UInt64) ENGINE = MergeTree ORDER BY c;
CREATE TABLE t2 (c UInt64) ENGINE = MergeTree ORDER BY c;

INSERT INTO t1 SELECT * FROM numbers(1000000);
INSERT INTO t2 SELECT * FROM numbers(1000000);

SELECT count()
FROM (SELECT * FROM t1) AS t1s,
     (SELECT * FROM t2 ORDER BY c ASC LIMIT 500000) AS t2s
WHERE t1s.c = t2s.c
SETTINGS
    max_rows_in_set_to_optimize_join = 100000000,
    join_algorithm = 'full_sorting_merge',
    query_plan_join_swap_table = 0,
    max_threads = 2;

DROP TABLE t1;
DROP TABLE t2;
