-- Fusing a filter into ARRAY JOIN must be skipped for LEFT ARRAY JOIN (optimizeReadInOrder keeps LIMIT
-- through it, but a fused filter drops rows) and when the plan may be serialized to older workers.

SET enable_analyzer = 1;
-- fusion is skipped for serialized plans, pin it so the plan-shape checks hold in the distributed-plan suite
SET serialize_query_plan = 0;

DROP TABLE IF EXISTS t_fuse_edge;
CREATE TABLE t_fuse_edge (id UInt64, arr Array(String)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_fuse_edge SELECT number, ['a', 'b', 'c'] FROM numbers(100);

-- INNER ARRAY JOIN is fused
SELECT countIf(explain LIKE '%Element filter%') = 1 FROM (EXPLAIN actions = 1 SELECT id FROM t_fuse_edge ARRAY JOIN arr AS elem WHERE elem = 'b');

-- LEFT ARRAY JOIN is not fused
SELECT countIf(explain LIKE '%Element filter%') = 0 FROM (EXPLAIN actions = 1 SELECT id FROM t_fuse_edge LEFT ARRAY JOIN arr AS elem WHERE elem = 'b');

-- LEFT ARRAY JOIN with ORDER BY ... LIMIT returns the right number of rows (would truncate if fused)
SELECT count() = 10 FROM (SELECT id FROM t_fuse_edge LEFT ARRAY JOIN arr AS elem WHERE elem = 'b' ORDER BY id LIMIT 10);
SELECT count() = 100 FROM (SELECT id FROM t_fuse_edge LEFT ARRAY JOIN arr AS elem WHERE elem = 'b');

-- fusion is suppressed when the plan is distributed or serialized
SELECT countIf(explain LIKE '%Element filter%') = 0 FROM (EXPLAIN actions = 1 SELECT id FROM t_fuse_edge ARRAY JOIN arr AS elem WHERE elem = 'b' SETTINGS make_distributed_plan = 1);
SELECT countIf(explain LIKE '%Element filter%') = 0 FROM (EXPLAIN actions = 1 SELECT id FROM t_fuse_edge ARRAY JOIN arr AS elem WHERE elem = 'b' SETTINGS serialize_query_plan = 1);

-- results stay correct in all cases
SELECT count() = 100 FROM (SELECT id FROM t_fuse_edge ARRAY JOIN arr AS elem WHERE elem = 'b');

DROP TABLE t_fuse_edge;
