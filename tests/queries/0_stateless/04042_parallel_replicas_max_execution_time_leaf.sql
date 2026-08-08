-- Tags: no-fasttest, no-old-analyzer
-- Tag no-fasttest: parallel replicas require a cluster that is not configured in the fast test.
-- Tag no-old-analyzer: parallel reading from replicas is only built on the analyzer code path, so
-- 'max_execution_time_leaf' does not apply with the old analyzer (the query runs as a plain local read).

DROP TABLE IF EXISTS test_max_execution_time_leaf SYNC;
CREATE TABLE test_max_execution_time_leaf
(
    key UInt64
)
ENGINE = ReplicatedMergeTree('/clickhouse/{database}/test_max_execution_time_leaf', 'r1')
ORDER BY key
SETTINGS index_granularity = 10;

SET max_rows_to_read = 0;
INSERT INTO test_max_execution_time_leaf SELECT number FROM numbers(1000);

SET allow_experimental_parallel_reading_from_replicas = 2, max_parallel_replicas = 3, cluster_for_parallel_replicas='test_cluster_one_shard_three_replicas_localhost';
SET use_query_cache = false;
-- Disable the automatic parallel replicas path so the explicit settings above are honoured (it would otherwise
-- override the cluster).
SET automatic_parallel_replicas_mode = 0;

-- Note: 'parallel_replicas_local_plan' is intentionally left at its default (1). When 'max_execution_time_leaf'
-- is set, the local plan is disabled automatically (the local replica shares the initiator's query status and
-- cannot be bounded by the leaf timeout separately), so the leaf timeout is effective for the default path too.

-- 'sleepEachRow' with 'max_block_size = 1' makes every replica spend a deterministic amount of wall-clock time while
-- reading, so the timeout fires reliably regardless of how fast the hardware is. The work is spread across replicas,
-- so each leaf accumulates several seconds of sleep, far exceeding the one second timeout.

-- 'max_threads = 1' makes each replica sleep serially. With a higher (default or randomized) 'max_threads',
-- the sleeps run in parallel reading streams and the query can finish before the one second timeout fires:
-- 1000 rows * 0.01 s over 3 replicas leaves only ~1.1 s of wall-clock time per replica already with 3 threads,
-- and far less on machines with more cores.
SET max_threads = 1;

-- A timeout may fire while a remote leaf query is still in the pending state (e.g. on a cold or heavily
-- loaded server, or a slow sanitizer/debug build): the kill then surfaces as QUERY_WAS_CANCELLED
-- ("Query is killed in pending state") instead of TIMEOUT_EXCEEDED, so both codes are accepted below.
-- A warm-up query (no timeouts involved) makes that race unlikely in the first place.
SELECT sum(key) FROM test_max_execution_time_leaf FORMAT Null;

-- The whole-query timeout 'max_execution_time' aborts the query.
SELECT sum(sleepEachRow(0.01)) FROM test_max_execution_time_leaf SETTINGS max_block_size = 1, max_execution_time = 1; -- { serverError TIMEOUT_EXCEEDED, QUERY_WAS_CANCELLED }
-- The leaf timeout 'max_execution_time_leaf' aborts the per-replica (leaf) execution.
SELECT sum(sleepEachRow(0.01)) FROM test_max_execution_time_leaf SETTINGS max_block_size = 1, max_execution_time_leaf = 1; -- { serverError TIMEOUT_EXCEEDED, QUERY_WAS_CANCELLED }
-- In 'break' mode a partial result is returned instead of an error.
SELECT sum(sleepEachRow(0.01)) FROM test_max_execution_time_leaf FORMAT Null SETTINGS max_block_size = 1, max_execution_time = 1, timeout_overflow_mode = 'break';
SELECT sum(sleepEachRow(0.01)) FROM test_max_execution_time_leaf FORMAT Null SETTINGS max_block_size = 1, max_execution_time_leaf = 1, timeout_overflow_mode_leaf = 'break';

-- The leaf timeout must win even when a larger whole-query 'max_execution_time' is also set: the leaf value is
-- substituted only into the per-replica (leaf) execution, while the outer value bounds the initiator. The query
-- below would finish in well under 100 seconds, so only the one second leaf timeout can abort it.
SELECT sum(sleepEachRow(0.01)) FROM test_max_execution_time_leaf SETTINGS max_block_size = 1, max_execution_time = 100, max_execution_time_leaf = 1; -- { serverError TIMEOUT_EXCEEDED, QUERY_WAS_CANCELLED }
-- The leaf 'timeout_overflow_mode' must also win over a differing outer 'timeout_overflow_mode': the leaf uses the
-- default 'throw' mode, so the query aborts even though the outer mode is 'break'.
SELECT sum(sleepEachRow(0.01)) FROM test_max_execution_time_leaf SETTINGS max_block_size = 1, max_execution_time = 100, max_execution_time_leaf = 1, timeout_overflow_mode = 'break'; -- { serverError TIMEOUT_EXCEEDED, QUERY_WAS_CANCELLED }

-- The local plan is disabled only when the leaf timeout is stricter than the initiator's own
-- 'max_execution_time': when the initiator's timeout is at most the leaf timeout, it already bounds the local
-- reading at least as tightly, so the local plan must be kept. (A profile that caps both settings to the same
-- value - like the one used by the Fast test job - must not lose the local plan for every query.)
-- 'parallel_replicas_local_plan' is pinned to 1 here because the test harness randomizes it.
SELECT countIf(explain LIKE '%ReadFromMergeTree%') > 0 FROM (EXPLAIN SELECT sum(key) FROM test_max_execution_time_leaf SETTINGS parallel_replicas_local_plan = 1, max_execution_time = 60, max_execution_time_leaf = 60);
-- A stricter leaf timeout still disables the local plan so that all leaf reading happens on remote replicas
-- where the leaf timeout is honored.
SELECT countIf(explain LIKE '%ReadFromMergeTree%') > 0 FROM (EXPLAIN SELECT sum(key) FROM test_max_execution_time_leaf SETTINGS parallel_replicas_local_plan = 1, max_execution_time = 60, max_execution_time_leaf = 1);

-- The leaf timeout is also effective for INSERT SELECT executed with parallel replicas. The local-pipeline
-- settings ('parallel_replicas_local_plan', 'parallel_replicas_insert_select_local_pipeline',
-- 'parallel_replicas_prefer_local_replica') are intentionally left at their defaults (1): when
-- 'max_execution_time_leaf' is set, the local insert select pipeline is skipped (it shares the initiator's
-- query status and cannot be bounded by the leaf timeout), so all leaf reading is bounded.
DROP TABLE IF EXISTS test_max_execution_time_leaf_insert SYNC;
CREATE TABLE test_max_execution_time_leaf_insert
(
    key UInt64
)
ENGINE = ReplicatedMergeTree('/clickhouse/{database}/test_max_execution_time_leaf_insert', 'r1')
ORDER BY key;

INSERT INTO test_max_execution_time_leaf_insert SELECT key + sleepEachRow(0.01) FROM test_max_execution_time_leaf SETTINGS parallel_distributed_insert_select = 2, max_block_size = 1, max_execution_time_leaf = 1; -- { serverError TIMEOUT_EXCEEDED, QUERY_WAS_CANCELLED }

-- The leaf timeout must win for INSERT SELECT too even when a larger whole-query 'max_execution_time' is set.
-- The query text SETTINGS are stripped from the remote sub-query, so the leaf value shipped in the context is not
-- overridden by the outer 'max_execution_time' on the remote replicas.
INSERT INTO test_max_execution_time_leaf_insert SELECT key + sleepEachRow(0.01) FROM test_max_execution_time_leaf SETTINGS parallel_distributed_insert_select = 2, max_block_size = 1, max_execution_time = 100, max_execution_time_leaf = 1; -- { serverError TIMEOUT_EXCEEDED, QUERY_WAS_CANCELLED }
-- The leaf 'timeout_overflow_mode' (default 'throw') must win over the outer 'break', so the INSERT aborts.
INSERT INTO test_max_execution_time_leaf_insert SELECT key + sleepEachRow(0.01) FROM test_max_execution_time_leaf SETTINGS parallel_distributed_insert_select = 2, max_block_size = 1, max_execution_time = 100, max_execution_time_leaf = 1, timeout_overflow_mode = 'break'; -- { serverError TIMEOUT_EXCEEDED, QUERY_WAS_CANCELLED }

-- A repeated outer 'max_execution_time' in the query text (ParserSetQuery keeps one entry per occurrence) must not
-- leave a second copy behind on the remote replica: every occurrence is stripped, not just the first.
INSERT INTO test_max_execution_time_leaf_insert SELECT key + sleepEachRow(0.01) FROM test_max_execution_time_leaf SETTINGS parallel_distributed_insert_select = 2, max_block_size = 1, max_execution_time = 100, max_execution_time = 100, max_execution_time_leaf = 1; -- { serverError TIMEOUT_EXCEEDED, QUERY_WAS_CANCELLED }

-- The stripping must be limited to the top-level SETTINGS of the query text. A timeout the user wrote inside a
-- nested subquery (the documented leaf-node pattern of 'max_execution_time_leaf') must survive on the remote
-- replicas. An IN-subquery is used because a FROM-subquery would disqualify the INSERT SELECT from the
-- parallel-replica path altogether ('isInsertSelectTrivialEnoughForDistributedExecution'); the IN column is not
-- part of the primary key, so the set is built only on the remote replicas (not during initiator planning). The
-- 100 second leaf timeout shipped in the context cannot fire, so only the one second timeout inside the subquery
-- can abort the query - and it must do so on a remote replica.
INSERT INTO test_max_execution_time_leaf_insert SELECT key FROM test_max_execution_time_leaf WHERE (key % 2) IN (SELECT number % 2 FROM numbers(300) WHERE sleepEachRow(0.01) = 0 SETTINGS max_block_size = 1, max_execution_time = 1) SETTINGS parallel_distributed_insert_select = 2, max_execution_time_leaf = 100; -- { serverError TIMEOUT_EXCEEDED, QUERY_WAS_CANCELLED }

-- When 'max_execution_time_leaf' is not set, the query text must not be rewritten at all: the nested subquery
-- keeps its user-authored timeout on the remote replicas. The local pipeline is disabled so that the set is built
-- (and the timeout can fire) only on the remote replicas.
INSERT INTO test_max_execution_time_leaf_insert SELECT key FROM test_max_execution_time_leaf WHERE (key % 2) IN (SELECT number % 2 FROM numbers(300) WHERE sleepEachRow(0.01) = 0 SETTINGS max_block_size = 1, max_execution_time = 1) SETTINGS parallel_distributed_insert_select = 2, parallel_replicas_insert_select_local_pipeline = 0; -- { serverError TIMEOUT_EXCEEDED, QUERY_WAS_CANCELLED }

DROP TABLE test_max_execution_time_leaf_insert SYNC;
DROP TABLE test_max_execution_time_leaf SYNC;
