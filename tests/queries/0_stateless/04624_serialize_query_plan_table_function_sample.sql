-- SAMPLE / SAMPLE ... OFFSET on a table function must survive the query-plan serialization
-- round-trip: they are serialized out-of-band of the table function AST (in the
-- ReadFromTableFunctionStep flags), and resolveStorages must restore them onto the table
-- expression it reconstructs on the worker. If they were dropped there, every sampled read
-- below would return the full row set (10000).

DROP TABLE IF EXISTS t_04624;
CREATE TABLE t_04624 (k UInt64) ENGINE = MergeTree ORDER BY intHash32(k) SAMPLE BY intHash32(k);
INSERT INTO t_04624 SELECT number FROM numbers(10000);

SET enable_analyzer = 1;
SET serialize_query_plan = 1;
SET prefer_localhost_replica = 0;

-- The sampling is deterministic (a fixed range of intHash32), so the counts are exact, and the
-- two complementary samples partition the table: 4972 + 5028 = 10000.
SELECT count() FROM cluster('test_shard_localhost', merge(currentDatabase(), '^t_04624$'));
SELECT count() FROM cluster('test_shard_localhost', merge(currentDatabase(), '^t_04624$')) SAMPLE 1/2;
SELECT count() FROM cluster('test_shard_localhost', merge(currentDatabase(), '^t_04624$')) SAMPLE 1/2 OFFSET 1/2;

DROP TABLE t_04624;
