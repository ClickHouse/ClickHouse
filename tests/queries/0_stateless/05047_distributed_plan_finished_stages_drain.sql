-- Tags: no-old-analyzer, no-flaky-check, no-asan, no-msan, no-tsan, no-ubsan, no-debug
-- no-flaky-check and the sanitizer exclusions: the check below is a wall-clock bound, so it
-- needs runs where machine time relates to real time; the flaky check runs many copies of this
-- test at once and sanitizer builds are an order of magnitude slower.

-- The distributed plan driver must pop every already-finished stage at once: stages finish
-- together at the end of the query, and a driver that takes one stage per poll interval turns
-- this ~40-stage plan into at least 4 seconds of idle waiting (100 ms per stage) regardless of
-- hardware, so the bound below cannot pass on such a driver.

SET make_distributed_plan = 1, distributed_plan_execute_locally = 1, enable_parallel_replicas = 0, max_rows_to_group_by = 0;

CREATE TABLE tt (k UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO tt SELECT number FROM numbers(100);

SELECT sum(c) FROM (SELECT count() AS c FROM tt WHERE k % 20 = 1 GROUP BY k % 3 UNION ALL SELECT count() AS c FROM tt WHERE k % 20 = 2 GROUP BY k % 3 UNION ALL SELECT count() AS c FROM tt WHERE k % 20 = 3 GROUP BY k % 3 UNION ALL SELECT count() AS c FROM tt WHERE k % 20 = 4 GROUP BY k % 3 UNION ALL SELECT count() AS c FROM tt WHERE k % 20 = 5 GROUP BY k % 3 UNION ALL SELECT count() AS c FROM tt WHERE k % 20 = 6 GROUP BY k % 3 UNION ALL SELECT count() AS c FROM tt WHERE k % 20 = 7 GROUP BY k % 3 UNION ALL SELECT count() AS c FROM tt WHERE k % 20 = 8 GROUP BY k % 3 UNION ALL SELECT count() AS c FROM tt WHERE k % 20 = 9 GROUP BY k % 3 UNION ALL SELECT count() AS c FROM tt WHERE k % 20 = 10 GROUP BY k % 3 UNION ALL SELECT count() AS c FROM tt WHERE k % 20 = 11 GROUP BY k % 3 UNION ALL SELECT count() AS c FROM tt WHERE k % 20 = 12 GROUP BY k % 3 UNION ALL SELECT count() AS c FROM tt WHERE k % 20 = 13 GROUP BY k % 3 UNION ALL SELECT count() AS c FROM tt WHERE k % 20 = 14 GROUP BY k % 3 UNION ALL SELECT count() AS c FROM tt WHERE k % 20 = 15 GROUP BY k % 3 UNION ALL SELECT count() AS c FROM tt WHERE k % 20 = 16 GROUP BY k % 3 UNION ALL SELECT count() AS c FROM tt WHERE k % 20 = 17 GROUP BY k % 3 UNION ALL SELECT count() AS c FROM tt WHERE k % 20 = 18 GROUP BY k % 3 UNION ALL SELECT count() AS c FROM tt WHERE k % 20 = 19 GROUP BY k % 3 UNION ALL SELECT count() AS c FROM tt WHERE k % 20 = 20 GROUP BY k % 3)
SETTINGS log_comment = '05047_finished_stages_drain';

SYSTEM FLUSH LOGS query_log;

-- The unfixed driver needs at least 100 ms x 40 stages > 4 seconds; the healthy run takes well
-- under a second even on slow builds, so the threshold cannot flap in either direction.
SELECT max(query_duration_ms) < 3000 FROM system.query_log
WHERE type = 'QueryFinish' AND is_initial_query AND current_database = currentDatabase()
    AND log_comment = '05047_finished_stages_drain' AND event_date >= yesterday()
SETTINGS make_distributed_plan = 0;
