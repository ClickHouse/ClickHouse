-- A subquery `body(...)` is executed at request time, so on a clustered read every shard would
-- re-execute the subquery locally and could send a different payload (and receive a response of a
-- different schema) than the one inferred on the initiator. `urlCluster` therefore rejects a
-- subquery body with `BAD_ARGUMENTS` before any connection is attempted, so the unroutable address
-- below is never contacted. A constant string body stays supported (it is identical on every
-- shard) and is not rejected by this guard.

-- Explicit structure: the rejection comes from the storage-creation guard.
SELECT * FROM urlCluster('test_shard_localhost', 'http://localhost:11111/test/data', 'JSONEachRow', 'x UInt8', body((SELECT 1))); -- { serverError BAD_ARGUMENTS }

-- Omitted structure: the rejection must fire before the schema-inference request.
SELECT * FROM urlCluster('test_shard_localhost', 'http://localhost:11111/test/data', 'JSONEachRow', body((SELECT 1))); -- { serverError BAD_ARGUMENTS }

-- Automatic format detection also must not send the request.
SELECT * FROM urlCluster('test_shard_localhost', 'http://localhost:11111/test/data', body((SELECT 1))); -- { serverError BAD_ARGUMENTS }

-- A subquery body with an explicit output format is rejected the same way.
SELECT * FROM urlCluster('test_shard_localhost', 'http://localhost:11111/test/data', 'JSONEachRow', 'x UInt8', body((SELECT 1), CSV)); -- { serverError BAD_ARGUMENTS }
