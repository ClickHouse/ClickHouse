-- A row policy on a `Memory` table, which applies the policy inside the reading step,
-- must be applied to a read shipped as a serialized query plan.

SET serialize_query_plan = 1, prefer_localhost_replica = 0;

DROP TABLE IF EXISTS t_memory_05259;
CREATE TABLE t_memory_05259 (x UInt64, y UInt64) ENGINE = Memory;
INSERT INTO t_memory_05259 SELECT number, number * 10 FROM numbers(10);

DROP ROW POLICY IF EXISTS policy_05259 ON t_memory_05259;
CREATE ROW POLICY policy_05259 ON t_memory_05259 FOR SELECT USING x % 2 = 0 TO CURRENT_USER;

SELECT x FROM cluster('test_shard_localhost', currentDatabase(), t_memory_05259) ORDER BY x;
SELECT count() FROM cluster('test_shard_localhost', currentDatabase(), t_memory_05259);
SELECT y FROM cluster('test_shard_localhost', currentDatabase(), t_memory_05259) WHERE y > 20 ORDER BY y;
SELECT y FROM cluster('test_shard_localhost', currentDatabase(), t_memory_05259) PREWHERE x < 5 ORDER BY y;

DROP ROW POLICY policy_05259 ON t_memory_05259;
DROP TABLE t_memory_05259;
