-- Tags: no-fasttest, no-shared-merge-tree
-- no-fasttest: three-replica distributed reads plus a fileCluster scan, against Fast test's 60
-- second per-file budget (limits_fast.yaml pins max_execution_time = 60). The fuzzer itself does
-- work there; 04305 measures a sibling skip in Fast test without this tag.
-- no-shared-merge-tree: the cluster used here points every replica at this same server, which
-- SharedMergeTree does not support as a parallel replicas topology.

-- The serverfuzz/stress profile sets ast_fuzzer_runs server-wide, which would fuzz every statement
-- here, including the measurements. Pin the baseline to 0 so only statements with an explicit
-- SETTINGS ast_fuzzer_runs > 0 fire it.
SET ast_fuzzer_runs = 0;
SET ast_fuzzer_any_query = 0;
SET send_logs_level = 'fatal';

-- The setup statements and the measurements below must not themselves run as distributed reads,
-- otherwise they add worker queries to what the assertions count. Each statement that is meant to
-- be a distributed read enables parallel replicas in its own SETTINGS clause.
SET enable_parallel_replicas = 0;
SET parallel_distributed_insert_select = 0;

-- The DROPs below must actually run. A retried attempt re-enters with the same database when one is
-- given on the command line, so a suppressed DROP leaves the objects an earlier attempt created and
-- the CREATEs then fail with TABLE_ALREADY_EXISTS.
SET ignore_drop_queries_probability = 0;

DROP TABLE IF EXISTS t05032_worker_src SYNC;
DROP TABLE IF EXISTS t05032_declared_src SYNC;
DROP TABLE IF EXISTS t05032_view_src SYNC;
DROP TABLE IF EXISTS t05032_dst SYNC;
DROP TABLE IF EXISTS t05032_events SYNC;
DROP VIEW IF EXISTS t05032_view SYNC;

-- Each arm reads through a source of its own, named after the arm, and is measured only over the
-- secondary queries that read that source. A table-backed arm selects them by exact membership in
-- `tables`, which the server fills from the resolved storage rather than from the statement text.
-- Text cannot identify a source here: the fuzzer rewrites its own copy of the statement, including
-- splicing a pooled string literal into the middle of a name, and a name is also a prefix of longer
-- names, so a wildcard match over `query` admits rows that read a different source.
-- Scoping per arm rather than per test is what keeps one arm's fuzzing out of
-- another's measurement: a fuzzed copy of a read can itself be rewritten into a distributed one
-- (wrapTableAsDistributed) or is a cluster read to begin with, so it produces secondary queries of
-- its own, and those land in whichever window is open when they finish. The arms therefore cannot
-- share a source - a shared one makes every arm's rows match every other arm's filter.
-- A table-backed arm also requires this database in `databases`, which holds on all of its worker
-- rows even though a worker's own current_database is the default one. The cluster table function arm
-- has neither: its workers report `_table_function` for both, identically for every copy of this
-- test, so only its file name identifies it and the match is anchored at both ends instead.
-- Each skip assertion is per initiator rather than over the whole arm: a worker executes the one
-- statement its initiator sent it, so a skipped arm has exactly one query shape under every
-- initiating query id, while a fuzzed worker rewrites its own statement and so contributes several.
-- Grouping by `initial_query_id` is therefore an invariant instead of a threshold, and it needs no
-- before/after difference: rows another copy of this test or an earlier run left behind sit under
-- their own initiating ids, which satisfy the invariant on their own whenever the skip holds.
-- Only groups headed by a statement a client sent are measured. A fuzzed copy of one of the reads
-- below is a distributed read in its own right, so it dispatches workers legitimately and the shapes
-- under it say nothing about the skip. The fuzzer executes such a copy internally, so its initiating
-- row carries `is_internal` while this test's own statement does not. The predicate has to select on
-- that head: a worker row carries the flag too, and the rows that do are exactly the ones an
-- unfixed server adds, so testing the rows would leave the assertion unable to fail.
CREATE TABLE t05032_worker_src (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id
    SETTINGS index_granularity = 8;
CREATE TABLE t05032_declared_src (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id
    SETTINGS index_granularity = 8;
CREATE TABLE t05032_view_src (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id
    SETTINGS index_granularity = 8;
CREATE TABLE t05032_dst (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE t05032_events (label String, workers Int64) ENGINE = Memory;
INSERT INTO t05032_worker_src SELECT number, number FROM numbers(2000);
INSERT INTO t05032_declared_src SELECT number, number FROM numbers(2000);
INSERT INTO t05032_view_src SELECT number, number FROM numbers(2000);

SYSTEM FLUSH LOGS query_log;
INSERT INTO t05032_events
SELECT 'worker_before', count()
FROM system.query_log
WHERE is_initial_query = 0 AND has(databases, currentDatabase())
  AND has(tables, currentDatabase() || '.t05032_worker_src');
INSERT INTO t05032_events
SELECT 'declared_before', count()
FROM system.query_log
WHERE is_initial_query = 0 AND has(databases, currentDatabase())
  AND has(tables, currentDatabase() || '.t05032_declared_src');
INSERT INTO t05032_events
SELECT 'cluster_before', count()
FROM system.query_log
WHERE is_initial_query = 0 AND position(query, '\'' || currentDatabase() || '_t05032_cluster_src.csv\'') > 0;
INSERT INTO t05032_events
SELECT 'view_before', count()
FROM system.query_log
WHERE is_initial_query = 0 AND has(databases, currentDatabase())
  AND has(tables, currentDatabase() || '.t05032_view_src');
INSERT INTO t05032_events
SELECT 'plain_before', count()
FROM system.query_log
WHERE is_initial_query = 0 AND startsWith(query, 'SELECT 1 AS t05032_plain_control');

-- A distributed INSERT ... SELECT over the parallel replicas cluster. Each replica executes the
-- statement as a worker for the initiator: it reads its assigned ranges through a coordination
-- channel that belongs to the initiator's read, and its copy of the statement carries this SETTINGS
-- clause. When such a worker finishes, the server-side AST fuzzer must not re-run the statement on
-- that context: a fuzzed copy would announce to the initiator's coordinator a second time under the
-- same replica number, which the coordinator rejects with a LOGICAL_ERROR.
INSERT INTO t05032_dst SELECT id, v FROM t05032_worker_src
SETTINGS enable_parallel_replicas = 1, max_parallel_replicas = 3,
         cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
         parallel_replicas_for_non_replicated_merge_tree = 1,
         parallel_distributed_insert_select = 2,
         parallel_replicas_local_plan = 0,
         parallel_replicas_insert_select_local_pipeline = 0,
         automatic_parallel_replicas_mode = 0, parallel_replicas_mode = 'read_tasks',
         enable_analyzer = 1,
         ast_fuzzer_runs = 5, ast_fuzzer_any_query = 1;

SYSTEM FLUSH LOGS query_log;
INSERT INTO t05032_events
SELECT 'worker_after', count()
FROM system.query_log
WHERE is_initial_query = 0 AND has(databases, currentDatabase())
  AND has(tables, currentDatabase() || '.t05032_worker_src');

-- The statement really did run as a distributed read: it produced worker queries against this arm's
-- source. Without them there is nothing for the skip to apply to and the next assertion would be
-- vacuous.
SELECT 'workers_ran',
      (SELECT workers FROM t05032_events WHERE label = 'worker_after')
    - (SELECT workers FROM t05032_events WHERE label = 'worker_before') > 0;

-- The workers were not fuzzed.
SELECT 'workers_not_fuzzed',
       (SELECT count() > 0 AND max(shapes) <= 1 FROM
            (SELECT uniqExact(normalized_query_hash) AS shapes
             FROM system.query_log
             WHERE is_initial_query = 0 AND has(databases, currentDatabase())
               AND has(tables, currentDatabase() || '.t05032_worker_src')
               AND initial_query_id IN (SELECT query_id FROM system.query_log
                                        WHERE is_initial_query = 1 AND is_internal = 0
                                          AND current_database = currentDatabase())
             GROUP BY initial_query_id));

-- Every source row arrived, so declining to fuzz the workers did not disturb the statement itself.
-- The row count is not asserted: the fuzzer re-executes mutated copies of the statement, which write
-- their own rows, so only the presence of the full source key range is stable here.
SELECT 'source_rows_written', count(DISTINCT id) = (SELECT count() FROM t05032_worker_src)
FROM t05032_dst WHERE id < (SELECT count() FROM t05032_worker_src);

-- Control: an ordinary statement is not a worker for anyone, so it produces no worker query. It
-- reads no table, so the fuzzer cannot turn it into a distributed read: wrapTableAsDistributed
-- rewrites a plain table reference into remote()/cluster() at random, which would let a fuzzed copy
-- of a table-backed control produce workers on correct code. The alias is what makes its rows
-- selectable, since a tableless statement names no source of ours.
-- This does not also prove the fuzzer ran: no per-invocation observable separated a fuzzing server
-- from a non-fuzzing one on measurement, because the fuzzer's own attempts are not attributable to
-- the query that triggered them (its finish callback is registered after the logging one, so the
-- initiator's row is already snapshotted). 03833 and 04344 cover the fuzzer being alive at all.
-- For the same reason the test does not pin down that the initiating distributed statement is still
-- fuzzed: the only per-invocation statistic for it, the number of distinct initiator shapes, was
-- measured at 5 to 7 with the skip and 3 to 6 without any fuzzing at all, so no threshold separates
-- them. What the arms here do establish is that a worker is not fuzzed while its read still returns
-- the right answer.
SELECT 1 AS t05032_plain_control
SETTINGS ast_fuzzer_runs = 5, ast_fuzzer_any_query = 1 FORMAT Null;

SYSTEM FLUSH LOGS query_log;
INSERT INTO t05032_events
SELECT 'plain_after', count()
FROM system.query_log
WHERE is_initial_query = 0 AND startsWith(query, 'SELECT 1 AS t05032_plain_control');

SELECT 'plain_query_no_workers',
      (SELECT workers FROM t05032_events WHERE label = 'plain_after')
    - (SELECT workers FROM t05032_events WHERE label = 'plain_before') = 0;

-- The other arms enable parallel replicas through the alias enable_parallel_replicas; this one uses
-- the declared name allow_experimental_parallel_reading_from_replicas and must reach the same skip.
-- The decision is taken from the context's own role, so no spelling of the setting re-enables the
-- fuzz run on a worker.
INSERT INTO t05032_dst SELECT id, v FROM t05032_declared_src
SETTINGS allow_experimental_parallel_reading_from_replicas = 1, max_parallel_replicas = 3,
         cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
         parallel_replicas_for_non_replicated_merge_tree = 1,
         parallel_distributed_insert_select = 2,
         parallel_replicas_local_plan = 0,
         parallel_replicas_insert_select_local_pipeline = 0,
         automatic_parallel_replicas_mode = 0, parallel_replicas_mode = 'read_tasks',
         enable_analyzer = 1,
         ast_fuzzer_runs = 5, ast_fuzzer_any_query = 1;

SYSTEM FLUSH LOGS query_log;
INSERT INTO t05032_events
SELECT 'declared_after', count()
FROM system.query_log
WHERE is_initial_query = 0 AND has(databases, currentDatabase())
  AND has(tables, currentDatabase() || '.t05032_declared_src');

SELECT 'declared_spelling_workers_ran',
      (SELECT workers FROM t05032_events WHERE label = 'declared_after')
    - (SELECT workers FROM t05032_events WHERE label = 'declared_before') > 0;

SELECT 'declared_spelling_not_fuzzed',
       (SELECT count() > 0 AND max(shapes) <= 1 FROM
            (SELECT uniqExact(normalized_query_hash) AS shapes
             FROM system.query_log
             WHERE is_initial_query = 0 AND has(databases, currentDatabase())
               AND has(tables, currentDatabase() || '.t05032_declared_src')
               AND initial_query_id IN (SELECT query_id FROM system.query_log
                                        WHERE is_initial_query = 1 AND is_internal = 0
                                          AND current_database = currentDatabase())
             GROUP BY initial_query_id));

-- A cluster table function marks its workers with the same context field, but hands them a task
-- iterator rather than a reading coordinator, so this arm covers the other channel the skip protects.
-- fileCluster is used because it needs no network. The file is written through file() first so the
-- test carries its own data. The name carries the database because user_files is one server-wide
-- directory with no database component, so concurrent copies of this test would otherwise share the
-- file, and because it is what makes this arm's worker queries selectable.
INSERT INTO FUNCTION file(currentDatabase() || '_t05032_cluster_src.csv', 'CSV', 'c1 UInt64, c2 UInt64')
SELECT number, number FROM numbers(500) SETTINGS engine_file_truncate_on_insert = 1;

SELECT 'cluster_function_read',
       sum(c2) = (SELECT sum(number) FROM numbers(500))
FROM fileCluster('test_cluster_one_shard_three_replicas_localhost',
                 currentDatabase() || '_t05032_cluster_src.csv',
                 'CSV', 'c1 UInt64, c2 UInt64')
-- enable_analyzer is pinned because the file name has to reach the workers already resolved: only
-- the analyzer folds currentDatabase() on the initiator, and a worker evaluating it locally reads
-- its own current_database instead, which is not where the file was written.
SETTINGS enable_analyzer = 1, ast_fuzzer_runs = 5, ast_fuzzer_any_query = 1;

SYSTEM FLUSH LOGS query_log;
INSERT INTO t05032_events
SELECT 'cluster_after', count()
FROM system.query_log
WHERE is_initial_query = 0 AND position(query, '\'' || currentDatabase() || '_t05032_cluster_src.csv\'') > 0;

SELECT 'cluster_function_workers_ran',
      (SELECT workers FROM t05032_events WHERE label = 'cluster_after')
    - (SELECT workers FROM t05032_events WHERE label = 'cluster_before') > 0;

-- This arm's own statement is a cluster read, so its fuzzed copies are initiators of worker queries
-- that match this filter legitimately.
SELECT 'cluster_function_not_fuzzed',
       (SELECT count() > 0 AND max(shapes) <= 1 FROM
            (SELECT uniqExact(normalized_query_hash) AS shapes
             FROM system.query_log
             WHERE is_initial_query = 0
               AND position(query, '\'' || currentDatabase() || '_t05032_cluster_src.csv\'') > 0
               AND initial_query_id IN (SELECT query_id FROM system.query_log
                                        WHERE is_initial_query = 1 AND is_internal = 0
                                          AND current_database = currentDatabase())
             GROUP BY initial_query_id));

-- A read through a view whose definer is resolved on a rebuilt context. Two properties: the read
-- succeeds, so the rebuilt context did not lose the coordination callback it needs; and its workers
-- are skipped too. The rebuilt context keeps the marker because getSQLSecurityOverriddenContext
-- starts from the global context but calls setClientInfo (StorageInMemoryMetadata.cpp:153-158).
-- The workers name the view's source rather than the view itself: the view is expanded before the
-- statement is sent to them.
CREATE VIEW t05032_view SQL SECURITY NONE AS SELECT id, v FROM t05032_view_src;

SELECT 'view_read', sum(v) FROM t05032_view
SETTINGS enable_parallel_replicas = 1, max_parallel_replicas = 3,
         cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
         parallel_replicas_for_non_replicated_merge_tree = 1,
         parallel_replicas_local_plan = 0,
         automatic_parallel_replicas_mode = 0, parallel_replicas_mode = 'read_tasks',
         enable_analyzer = 1,
         ast_fuzzer_runs = 5, ast_fuzzer_any_query = 1;

SYSTEM FLUSH LOGS query_log;
INSERT INTO t05032_events
SELECT 'view_after', count()
FROM system.query_log
WHERE is_initial_query = 0 AND has(databases, currentDatabase())
  AND has(tables, currentDatabase() || '.t05032_view_src');

SELECT 'view_workers_ran',
      (SELECT workers FROM t05032_events WHERE label = 'view_after')
    - (SELECT workers FROM t05032_events WHERE label = 'view_before') > 0;

SELECT 'view_not_fuzzed',
       (SELECT count() > 0 AND max(shapes) <= 1 FROM
            (SELECT uniqExact(normalized_query_hash) AS shapes
             FROM system.query_log
             WHERE is_initial_query = 0 AND has(databases, currentDatabase())
               AND has(tables, currentDatabase() || '.t05032_view_src')
               AND initial_query_id IN (SELECT query_id FROM system.query_log
                                        WHERE is_initial_query = 1 AND is_internal = 0
                                          AND current_database = currentDatabase())
             GROUP BY initial_query_id));

-- Server is alive after every arm above.
SELECT 'alive';

DROP VIEW t05032_view;
DROP TABLE t05032_events;
DROP TABLE t05032_dst;
DROP TABLE t05032_view_src;
DROP TABLE t05032_declared_src;
DROP TABLE t05032_worker_src;
