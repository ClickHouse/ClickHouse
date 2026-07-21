-- Tags: no-parallel-replicas
-- `STREAM` is not supported with parallel replicas (`ILLEGAL_STREAM`), so this test cannot run there.

-- Regression test for a use-after-free on the shared storage snapshot.
-- `ReadFromMergeTree::initializePipeline` used to replace `storage_snapshot->data` in place
-- when stripping data parts for a streaming query, while the snapshot object is shared
-- across the whole query (`enable_shared_storage_snapshot_in_query`), so a concurrent reader
-- of the same snapshot (here: `mergeTreeAnalyzeIndexes` in the other `UNION ALL` branch)
-- could read freed memory. Found by AST fuzzer.

DROP TABLE IF EXISTS t_snapshot_strip_race;
CREATE TABLE t_snapshot_strip_race (key Int32) ENGINE = MergeTree ORDER BY key;
INSERT INTO t_snapshot_strip_race SELECT number FROM numbers(100);

SET enable_streaming_queries = 1;
SET max_execution_time = 2;
-- The bug only manifests when the `StorageSnapshot` is shared across the query, which is what
-- this setting enables. Pin it explicitly so the test exercises the buggy path regardless of the
-- `compatibility` profile default (which is `0` in older profiles).
SET enable_shared_storage_snapshot_in_query = 1;

-- The streaming subquery never finishes, so the query ends with TIMEOUT_EXCEEDED;
-- before the fix, a sanitizer build reported a heap-use-after-free here instead.
SELECT * FROM mergeTreeAnalyzeIndexes(currentDatabase(), t_snapshot_strip_race, (key IN (SELECT key FROM t_snapshot_strip_race ORDER BY key DESC NULLS LAST LIMIT 10)))
UNION ALL
SELECT * FROM mergeTreeAnalyzeIndexes(currentDatabase(), t_snapshot_strip_race, (key IN (SELECT key FROM t_snapshot_strip_race STREAM ORDER BY key DESC NULLS LAST LIMIT 10)))
GROUP BY ALL
FORMAT Null; -- { serverError TIMEOUT_EXCEEDED }

DROP TABLE t_snapshot_strip_race;
