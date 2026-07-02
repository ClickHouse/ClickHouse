-- Regression test: INSERT/SELECT via cluster()/remote() into a parameterized view
-- should return a proper error, not crash with LOGICAL_ERROR '!res.empty()'.

CREATE VIEW pv AS SELECT 1 AS x WHERE x = {p:UInt32};

-- These should produce errors (parameterized view has no columns), not server crashes.
-- The empty column list from the local shard causes getStructureOfRemoteTable
-- to fall through to the remote shard path.
SELECT * FROM cluster('test_shard_localhost', currentDatabase(), 'pv'); -- { serverError NO_REMOTE_SHARD_AVAILABLE }
SELECT * FROM remote('127.0.0.1:9000', currentDatabase(), 'pv'); -- { serverError NO_REMOTE_SHARD_AVAILABLE }

INSERT INTO FUNCTION cluster('test_shard_localhost', currentDatabase(), 'pv') SELECT 1; -- { serverError NO_REMOTE_SHARD_AVAILABLE }
INSERT INTO FUNCTION remote('127.0.0.1:9000', currentDatabase(), 'pv') SELECT 1; -- { serverError NO_REMOTE_SHARD_AVAILABLE }

DROP VIEW pv;
