DROP TABLE IF EXISTS t1;
CREATE TABLE t1 (x Int32) ENGINE = Memory;

-- insert several blocks with 1 or 2 rows:
INSERT INTO t1 VALUES (1);
INSERT INTO t1 VALUES (10),(100);
INSERT INTO t1 VALUES (1000);
INSERT INTO t1 VALUES (10000),(100000);

SET max_rows_in_join = 111;
-- `ConstantJoin` spills only the build side, and the runner randomizes which side the planner picks.
SET query_plan_join_swap_table = 0;

SELECT x, sum(number), count(), FROM (
    SELECT t1.x, t2.number
    FROM t1
    CROSS JOIN numbers_mt(100_001) t2
    WHERE number <= x
)
GROUP BY ALL
ORDER BY x
SETTINGS log_comment = '03151_external_cross_join_spilled'
;

SYSTEM FLUSH LOGS query_log;
SELECT max(ProfileEvents['ExternalJoinWritePart']) > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish'
  AND log_comment = '03151_external_cross_join_spilled';
