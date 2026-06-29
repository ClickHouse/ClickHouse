-- Tags: shard
-- Test that remote() with nested merge() table function works with the analyzer.
-- https://github.com/ClickHouse/ClickHouse/issues/84672

DROP TABLE IF EXISTS test_t1;
DROP TABLE IF EXISTS test_t2;

CREATE TABLE test_t1 (x UInt64) ENGINE = Memory;
CREATE TABLE test_t2 (x UInt64) ENGINE = Memory;

INSERT INTO test_t1 VALUES (1), (2);
INSERT INTO test_t2 VALUES (3), (4);

-- merge() nested inside remote() should be sent to the remote server, not resolved locally.
SELECT sum(x) FROM remote('127.0.0.1', merge(currentDatabase(), '^test_t'));

-- Verify that merge()'s arguments are NOT resolved on the initiator.
-- Without the fix, the analyzer resolves merge() locally, which constant-folds
-- currentDatabase() into a CONSTANT node (with an EXPRESSION subtree preserving
-- the original function). With the fix, merge() arguments are left unresolved,
-- so no constant folding occurs and no EXPRESSION subtree appears.
SELECT count() = 0 FROM (EXPLAIN QUERY TREE SELECT sum(x) FROM remote('127.0.0.1', merge(currentDatabase(), '^test_t'))) WHERE explain LIKE '%EXPRESSION%'
SETTINGS enable_analyzer = 1;

DROP TABLE test_t1;
DROP TABLE test_t2;
