-- Random settings limits: enable_parallel_replicas=(0, 0)
-- The regression this test guards against is a re-preparation of the `IN` set once per level of
-- nesting. Instead of relying on the test's time budget to notice it, the number of rows read is
-- checked directly below: the set has to be prepared exactly once, no matter how deep the nesting.
-- That also lets the nesting stay well away from the 5 % of a thread's stack that `checkStackSize`
-- allows under TSan, which 20 levels of subquery analysis already exceed.

DROP TABLE IF EXISTS mergetree_00673;

CREATE TABLE mergetree_00673 (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO mergetree_00673 VALUES (1);

SELECT * FROM (SELECT * FROM (SELECT * FROM (SELECT * FROM (SELECT * FROM (SELECT * FROM (SELECT * FROM (SELECT * FROM (SELECT * FROM (SELECT * FROM (SELECT * FROM (SELECT * FROM (SELECT * FROM (SELECT * FROM (SELECT * FROM mergetree_00673 WHERE x IN (SELECT * FROM numbers(1000000)))))))))))))))) SETTINGS log_queries = 1;

SET force_primary_key = 1;

SELECT * FROM (SELECT * FROM (SELECT * FROM (SELECT * FROM (SELECT * FROM (SELECT * FROM (SELECT * FROM (SELECT * FROM (SELECT * FROM (SELECT * FROM (SELECT * FROM (SELECT * FROM (SELECT * FROM (SELECT * FROM (SELECT * FROM mergetree_00673 WHERE x IN (SELECT * FROM numbers(1000000)))))))))))))))) SETTINGS log_queries = 1;

DROP TABLE mergetree_00673;

-- One preparation of the set reads 1000000 rows from `numbers` plus a single row from the table;
-- a per-level preparation would read a multiple of that.
SET force_primary_key = 0;
SYSTEM FLUSH LOGS query_log;

SELECT max(read_rows) < 2000000 AND count() >= 2
FROM system.query_log
WHERE current_database = currentDatabase()
    AND type = 'QueryFinish'
    AND query LIKE 'SELECT * FROM (SELECT%mergetree_00673%';
