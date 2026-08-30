-- Tags: no-old-analyzer
-- no-old-analyzer: make_distributed_plan requires the analyzer.

-- A ~40-stage distributed plan executed in process. That the driver collects all finished stages in
-- one poll is not asserted: from SQL it shows up only as latency, which is not separable from the
-- load of the parallel suite.

SET make_distributed_plan = 1, distributed_plan_execute_locally = 1, enable_parallel_replicas = 0, max_rows_to_group_by = 0;

CREATE TABLE tt (k UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO tt SELECT number FROM numbers(100);

SELECT sum(c) FROM (SELECT count() AS c FROM tt WHERE k % 20 = 1 GROUP BY k % 3 UNION ALL SELECT count() AS c FROM tt WHERE k % 20 = 2 GROUP BY k % 3 UNION ALL SELECT count() AS c FROM tt WHERE k % 20 = 3 GROUP BY k % 3 UNION ALL SELECT count() AS c FROM tt WHERE k % 20 = 4 GROUP BY k % 3 UNION ALL SELECT count() AS c FROM tt WHERE k % 20 = 5 GROUP BY k % 3 UNION ALL SELECT count() AS c FROM tt WHERE k % 20 = 6 GROUP BY k % 3 UNION ALL SELECT count() AS c FROM tt WHERE k % 20 = 7 GROUP BY k % 3 UNION ALL SELECT count() AS c FROM tt WHERE k % 20 = 8 GROUP BY k % 3 UNION ALL SELECT count() AS c FROM tt WHERE k % 20 = 9 GROUP BY k % 3 UNION ALL SELECT count() AS c FROM tt WHERE k % 20 = 10 GROUP BY k % 3 UNION ALL SELECT count() AS c FROM tt WHERE k % 20 = 11 GROUP BY k % 3 UNION ALL SELECT count() AS c FROM tt WHERE k % 20 = 12 GROUP BY k % 3 UNION ALL SELECT count() AS c FROM tt WHERE k % 20 = 13 GROUP BY k % 3 UNION ALL SELECT count() AS c FROM tt WHERE k % 20 = 14 GROUP BY k % 3 UNION ALL SELECT count() AS c FROM tt WHERE k % 20 = 15 GROUP BY k % 3 UNION ALL SELECT count() AS c FROM tt WHERE k % 20 = 16 GROUP BY k % 3 UNION ALL SELECT count() AS c FROM tt WHERE k % 20 = 17 GROUP BY k % 3 UNION ALL SELECT count() AS c FROM tt WHERE k % 20 = 18 GROUP BY k % 3 UNION ALL SELECT count() AS c FROM tt WHERE k % 20 = 19 GROUP BY k % 3 UNION ALL SELECT count() AS c FROM tt WHERE k % 20 = 20 GROUP BY k % 3);
