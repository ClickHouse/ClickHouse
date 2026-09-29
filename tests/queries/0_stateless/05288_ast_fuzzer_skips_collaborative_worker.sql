-- Tags: no-shared-merge-tree
-- no-shared-merge-tree: the parallel replicas cluster used here points every replica at this server.

-- The server-side AST fuzzer (`ast_fuzzer_runs`) must not re-run a statement on a node that executes it
-- as a worker for another node's query, a parallel replicas participant or a cluster table function
-- worker: a fuzzed copy would take part in the initiator's read a second time. The initiator is fuzzed.

-- Only the statements that set `ast_fuzzer_runs` themselves are fuzzed, never the measurements.
SET ast_fuzzer_runs = 0;
SET ast_fuzzer_any_query = 0;
SET send_logs_level = 'fatal';

-- Only the statements under test are distributed reads; nothing else may add worker queries.
SET enable_parallel_replicas = 0;
SET parallel_distributed_insert_select = 0;

-- The `DROP`s must run, or a retry in the same database fails on `CREATE`.
SET ignore_drop_queries_probability = 0;

DROP TABLE IF EXISTS t05288_worker_src SYNC;
DROP TABLE IF EXISTS t05288_declared_src SYNC;
DROP TABLE IF EXISTS t05288_view_src SYNC;
DROP TABLE IF EXISTS t05288_dst SYNC;
DROP TABLE IF EXISTS t05288_events SYNC;
DROP VIEW IF EXISTS t05288_view SYNC;

-- Each arm reads a source of its own and is measured over the worker queries that read it, selected by
-- `tables` and `databases` (fuzzed copies rewrite query text, so text cannot identify a source), or by
-- the file name for the cluster table function. The assertions group the workers by the initiating query
-- a client sent: an unfuzzed worker executes exactly the statement it was sent, so each group has one
-- query shape and no internal rows. Groups headed by a fuzzed copy are not measured: such a copy is a
-- distributed read of its own and dispatches workers legitimately.
CREATE TABLE t05288_worker_src (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id
    SETTINGS index_granularity = 8;
CREATE TABLE t05288_declared_src (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id
    SETTINGS index_granularity = 8;
CREATE TABLE t05288_view_src (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id
    SETTINGS index_granularity = 8;
CREATE TABLE t05288_dst (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE t05288_events (label String, workers Int64) ENGINE = Memory;
INSERT INTO t05288_worker_src SELECT number, number FROM numbers(2000);
INSERT INTO t05288_declared_src SELECT number, number FROM numbers(2000);
INSERT INTO t05288_view_src SELECT number, number FROM numbers(2000);

SYSTEM FLUSH LOGS query_log;
INSERT INTO t05288_events
SELECT 'worker_before', count()
FROM system.query_log
WHERE event_date >= yesterday() AND is_initial_query = 0 AND has(databases, currentDatabase())
  AND has(tables, currentDatabase() || '.t05288_worker_src');
INSERT INTO t05288_events
SELECT 'declared_before', count()
FROM system.query_log
WHERE event_date >= yesterday() AND is_initial_query = 0 AND has(databases, currentDatabase())
  AND has(tables, currentDatabase() || '.t05288_declared_src');
INSERT INTO t05288_events
SELECT 'cluster_before', count()
FROM system.query_log
WHERE event_date >= yesterday() AND is_initial_query = 0 AND position(query, '\'' || currentDatabase() || '_t05288_cluster_src.csv\'') > 0;
INSERT INTO t05288_events
SELECT 'view_before', count()
FROM system.query_log
WHERE event_date >= yesterday() AND is_initial_query = 0 AND has(databases, currentDatabase())
  AND has(tables, currentDatabase() || '.t05288_view_src');
INSERT INTO t05288_events
SELECT 'plain_before', count()
FROM system.query_log
WHERE event_date >= yesterday() AND is_initial_query = 0 AND startsWith(query, 'SELECT 1 AS t05288_plain_control');
-- A fuzzed copy runs internally, with an initiating row of its own naming the arm's source.
INSERT INTO t05288_events
SELECT 'worker_fuzz_copies_before', count()
FROM system.query_log
WHERE event_date >= yesterday() AND is_initial_query = 1 AND is_internal = 1
  AND has(tables, currentDatabase() || '.t05288_worker_src');
-- Each arm brackets its statement with the skip counter. It is server-wide, so concurrent queries can
-- only inflate a delta.
INSERT INTO t05288_events
SELECT 'skips_before_worker',
       (SELECT ifNull(sum(value), 0) FROM system.events
        WHERE event = 'ASTFuzzerSkippedCollaborativeWorker');

-- A distributed `INSERT ... SELECT`: every replica executes it as a worker, `SETTINGS` clause included.
INSERT INTO t05288_dst SELECT id, v FROM t05288_worker_src
SETTINGS enable_parallel_replicas = 1, max_parallel_replicas = 3,
         cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
         parallel_replicas_for_non_replicated_merge_tree = 1,
         parallel_distributed_insert_select = 2,
         parallel_replicas_local_plan = 0,
         parallel_replicas_insert_select_local_pipeline = 0,
         automatic_parallel_replicas_mode = 0, parallel_replicas_mode = 'read_tasks',
         ast_fuzzer_runs = 5, ast_fuzzer_any_query = 1;

SYSTEM FLUSH LOGS query_log;
INSERT INTO t05288_events
SELECT 'worker_after', count()
FROM system.query_log
WHERE event_date >= yesterday() AND is_initial_query = 0 AND has(databases, currentDatabase())
  AND has(tables, currentDatabase() || '.t05288_worker_src');

-- The statement ran as a distributed read, so the skip had workers to apply to.
SELECT 'workers_ran',
      (SELECT workers FROM t05288_events WHERE label = 'worker_after')
    - (SELECT workers FROM t05288_events WHERE label = 'worker_before') > 0;

-- The fuzzer ran on this statement, so the skip assertions cannot pass because nothing was fuzzed.
INSERT INTO t05288_events
SELECT 'worker_fuzz_copies_after', count()
FROM system.query_log
WHERE event_date >= yesterday() AND is_initial_query = 1 AND is_internal = 1
  AND has(tables, currentDatabase() || '.t05288_worker_src');

SELECT 'initiator_was_fuzzed',
      (SELECT workers FROM t05288_events WHERE label = 'worker_fuzz_copies_after')
    - (SELECT workers FROM t05288_events WHERE label = 'worker_fuzz_copies_before') > 0;

-- The workers were not fuzzed.
SELECT 'workers_not_fuzzed',
       (SELECT count() > 0 AND max(shapes) <= 1 FROM
            (SELECT uniqExact(normalized_query_hash) AS shapes
             FROM system.query_log
             WHERE event_date >= yesterday() AND is_initial_query = 0 AND has(databases, currentDatabase())
               AND has(tables, currentDatabase() || '.t05288_worker_src')
               AND initial_query_id IN (SELECT query_id FROM system.query_log
                                        WHERE event_date >= yesterday() AND is_initial_query = 1 AND is_internal = 0
                                          AND current_database = currentDatabase())
             GROUP BY initial_query_id));

SELECT 'worker_skips_recorded',
       (SELECT ifNull(sum(value), 0) FROM system.events
        WHERE event = 'ASTFuzzerSkippedCollaborativeWorker')
     - (SELECT workers FROM t05288_events WHERE label = 'skips_before_worker') > 0;

-- A fuzzed copy of a worker's statement would be an internal row under the same initiating query.
SELECT 'worker_no_internal_workers',
       (SELECT count() FROM system.query_log
        WHERE event_date >= yesterday() AND is_initial_query = 0 AND is_internal = 1
          AND has(databases, currentDatabase())
          AND has(tables, currentDatabase() || '.t05288_worker_src')
          AND initial_query_id IN (SELECT query_id FROM system.query_log
                                   WHERE event_date >= yesterday() AND is_initial_query = 1 AND is_internal = 0
                                     AND current_database = currentDatabase())) = 0;

-- Every source row arrived. The row count is not asserted: fuzzed copies of the `INSERT` write rows too.
SELECT 'source_rows_written', count(DISTINCT id) = (SELECT count() FROM t05288_worker_src)
FROM t05288_dst WHERE id < (SELECT count() FROM t05288_worker_src);

-- Control: an ordinary statement produces no worker queries. It reads no table, because the fuzzer can
-- rewrite a table read into a distributed one; the alias makes its rows selectable.
SELECT 1 AS t05288_plain_control
SETTINGS ast_fuzzer_runs = 5, ast_fuzzer_any_query = 1 FORMAT Null;

SYSTEM FLUSH LOGS query_log;
INSERT INTO t05288_events
SELECT 'plain_after', count()
FROM system.query_log
WHERE event_date >= yesterday() AND is_initial_query = 0 AND startsWith(query, 'SELECT 1 AS t05288_plain_control');

SELECT 'plain_query_no_workers',
      (SELECT workers FROM t05288_events WHERE label = 'plain_after')
    - (SELECT workers FROM t05288_events WHERE label = 'plain_before') = 0;

-- The same read, enabling parallel replicas by the setting's other name.
INSERT INTO t05288_events
SELECT 'skips_before_declared',
       (SELECT ifNull(sum(value), 0) FROM system.events
        WHERE event = 'ASTFuzzerSkippedCollaborativeWorker');
INSERT INTO t05288_events
SELECT 'declared_fuzz_copies_before', count()
FROM system.query_log
WHERE event_date >= yesterday() AND is_initial_query = 1 AND is_internal = 1
  AND has(tables, currentDatabase() || '.t05288_declared_src');

INSERT INTO t05288_dst SELECT id, v FROM t05288_declared_src
SETTINGS allow_experimental_parallel_reading_from_replicas = 1, max_parallel_replicas = 3,
         cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
         parallel_replicas_for_non_replicated_merge_tree = 1,
         parallel_distributed_insert_select = 2,
         parallel_replicas_local_plan = 0,
         parallel_replicas_insert_select_local_pipeline = 0,
         automatic_parallel_replicas_mode = 0, parallel_replicas_mode = 'read_tasks',
         ast_fuzzer_runs = 5, ast_fuzzer_any_query = 1;

SYSTEM FLUSH LOGS query_log;
INSERT INTO t05288_events
SELECT 'declared_after', count()
FROM system.query_log
WHERE event_date >= yesterday() AND is_initial_query = 0 AND has(databases, currentDatabase())
  AND has(tables, currentDatabase() || '.t05288_declared_src');

SELECT 'declared_spelling_workers_ran',
      (SELECT workers FROM t05288_events WHERE label = 'declared_after')
    - (SELECT workers FROM t05288_events WHERE label = 'declared_before') > 0;

INSERT INTO t05288_events
SELECT 'declared_fuzz_copies_after', count()
FROM system.query_log
WHERE event_date >= yesterday() AND is_initial_query = 1 AND is_internal = 1
  AND has(tables, currentDatabase() || '.t05288_declared_src');

SELECT 'declared_initiator_was_fuzzed',
      (SELECT workers FROM t05288_events WHERE label = 'declared_fuzz_copies_after')
    - (SELECT workers FROM t05288_events WHERE label = 'declared_fuzz_copies_before') > 0;

SELECT 'declared_spelling_not_fuzzed',
       (SELECT count() > 0 AND max(shapes) <= 1 FROM
            (SELECT uniqExact(normalized_query_hash) AS shapes
             FROM system.query_log
             WHERE event_date >= yesterday() AND is_initial_query = 0 AND has(databases, currentDatabase())
               AND has(tables, currentDatabase() || '.t05288_declared_src')
               AND initial_query_id IN (SELECT query_id FROM system.query_log
                                        WHERE event_date >= yesterday() AND is_initial_query = 1 AND is_internal = 0
                                          AND current_database = currentDatabase())
             GROUP BY initial_query_id));

SELECT 'declared_skips_recorded',
       (SELECT ifNull(sum(value), 0) FROM system.events
        WHERE event = 'ASTFuzzerSkippedCollaborativeWorker')
     - (SELECT workers FROM t05288_events WHERE label = 'skips_before_declared') > 0;

SELECT 'declared_no_internal_workers',
       (SELECT count() FROM system.query_log
        WHERE event_date >= yesterday() AND is_initial_query = 0 AND is_internal = 1
          AND has(databases, currentDatabase())
          AND has(tables, currentDatabase() || '.t05288_declared_src')
          AND initial_query_id IN (SELECT query_id FROM system.query_log
                                   WHERE event_date >= yesterday() AND is_initial_query = 1 AND is_internal = 0
                                     AND current_database = currentDatabase())) = 0;

-- A cluster table function worker is a collaborative worker too: it reads the tasks the initiator
-- hands out. The file name carries the database, since `user_files` is shared by all databases.
INSERT INTO FUNCTION file(currentDatabase() || '_t05288_cluster_src.csv', 'CSV', 'c1 UInt64, c2 UInt64')
SELECT number, number FROM numbers(500) SETTINGS engine_file_truncate_on_insert = 1;

INSERT INTO t05288_events
SELECT 'skips_before_cluster',
       (SELECT ifNull(sum(value), 0) FROM system.events
        WHERE event = 'ASTFuzzerSkippedCollaborativeWorker');
-- A fuzzed copy still has the `currentDatabase` call, so it is matched by the file name suffix or the
-- table function, and by `current_database`, since copies of other queries can name this file too.
INSERT INTO t05288_events
SELECT 'cluster_fuzz_copies_before', count()
FROM system.query_log
WHERE event_date >= yesterday() AND is_initial_query = 1 AND is_internal = 1 AND current_database = currentDatabase()
  AND (position(query, '_t05288_cluster_src.csv') > 0
       OR has(tables, '_table_function.fileCluster'));

SELECT 'cluster_function_read',
       sum(c2) = (SELECT sum(number) FROM numbers(500))
FROM fileCluster('test_cluster_one_shard_three_replicas_localhost',
                 currentDatabase() || '_t05288_cluster_src.csv',
                 'CSV', 'c1 UInt64, c2 UInt64')
SETTINGS ast_fuzzer_runs = 5, ast_fuzzer_any_query = 1;

SYSTEM FLUSH LOGS query_log;
INSERT INTO t05288_events
SELECT 'cluster_after', count()
FROM system.query_log
WHERE event_date >= yesterday() AND is_initial_query = 0 AND position(query, '\'' || currentDatabase() || '_t05288_cluster_src.csv\'') > 0;

SELECT 'cluster_function_workers_ran',
      (SELECT workers FROM t05288_events WHERE label = 'cluster_after')
    - (SELECT workers FROM t05288_events WHERE label = 'cluster_before') > 0;

INSERT INTO t05288_events
SELECT 'cluster_fuzz_copies_after', count()
FROM system.query_log
WHERE event_date >= yesterday() AND is_initial_query = 1 AND is_internal = 1 AND current_database = currentDatabase()
  AND (position(query, '_t05288_cluster_src.csv') > 0
       OR has(tables, '_table_function.fileCluster'));

SELECT 'cluster_initiator_was_fuzzed',
      (SELECT workers FROM t05288_events WHERE label = 'cluster_fuzz_copies_after')
    - (SELECT workers FROM t05288_events WHERE label = 'cluster_fuzz_copies_before') > 0;

SELECT 'cluster_function_not_fuzzed',
       (SELECT count() > 0 AND max(shapes) <= 1 FROM
            (SELECT uniqExact(normalized_query_hash) AS shapes
             FROM system.query_log
             WHERE event_date >= yesterday() AND is_initial_query = 0
               AND position(query, '\'' || currentDatabase() || '_t05288_cluster_src.csv\'') > 0
               AND initial_query_id IN (SELECT query_id FROM system.query_log
                                        WHERE event_date >= yesterday() AND is_initial_query = 1 AND is_internal = 0
                                          AND current_database = currentDatabase())
             GROUP BY initial_query_id));

SELECT 'cluster_skips_recorded',
       (SELECT ifNull(sum(value), 0) FROM system.events
        WHERE event = 'ASTFuzzerSkippedCollaborativeWorker')
     - (SELECT workers FROM t05288_events WHERE label = 'skips_before_cluster') > 0;

SELECT 'cluster_no_internal_workers',
       (SELECT count() FROM system.query_log
        WHERE event_date >= yesterday() AND is_initial_query = 0 AND is_internal = 1
          AND position(query, '\'' || currentDatabase() || '_t05288_cluster_src.csv\'') > 0
          AND initial_query_id IN (SELECT query_id FROM system.query_log
                                   WHERE event_date >= yesterday() AND is_initial_query = 1 AND is_internal = 0
                                     AND current_database = currentDatabase())) = 0;

-- A read through an `SQL SECURITY NONE` view, executed in a separately built context: the read succeeds
-- and its workers, which read the view's source, are skipped too.
CREATE VIEW t05288_view SQL SECURITY NONE AS SELECT id, v FROM t05288_view_src;

INSERT INTO t05288_events
SELECT 'skips_before_view',
       (SELECT ifNull(sum(value), 0) FROM system.events
        WHERE event = 'ASTFuzzerSkippedCollaborativeWorker');
-- Fuzzed copies of this statement name the view itself.
INSERT INTO t05288_events
SELECT 'view_fuzz_copies_before', count()
FROM system.query_log
WHERE event_date >= yesterday() AND is_initial_query = 1 AND is_internal = 1
  AND has(tables, currentDatabase() || '.t05288_view');

SELECT 'view_read', sum(v) FROM t05288_view
SETTINGS enable_parallel_replicas = 1, max_parallel_replicas = 3,
         cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
         parallel_replicas_for_non_replicated_merge_tree = 1,
         parallel_replicas_local_plan = 0,
         automatic_parallel_replicas_mode = 0, parallel_replicas_mode = 'read_tasks',
         ast_fuzzer_runs = 5, ast_fuzzer_any_query = 1;

SYSTEM FLUSH LOGS query_log;
INSERT INTO t05288_events
SELECT 'view_after', count()
FROM system.query_log
WHERE event_date >= yesterday() AND is_initial_query = 0 AND has(databases, currentDatabase())
  AND has(tables, currentDatabase() || '.t05288_view_src');

SELECT 'view_workers_ran',
      (SELECT workers FROM t05288_events WHERE label = 'view_after')
    - (SELECT workers FROM t05288_events WHERE label = 'view_before') > 0;

INSERT INTO t05288_events
SELECT 'view_fuzz_copies_after', count()
FROM system.query_log
WHERE event_date >= yesterday() AND is_initial_query = 1 AND is_internal = 1
  AND has(tables, currentDatabase() || '.t05288_view');

SELECT 'view_initiator_was_fuzzed',
      (SELECT workers FROM t05288_events WHERE label = 'view_fuzz_copies_after')
    - (SELECT workers FROM t05288_events WHERE label = 'view_fuzz_copies_before') > 0;

SELECT 'view_not_fuzzed',
       (SELECT count() > 0 AND max(shapes) <= 1 FROM
            (SELECT uniqExact(normalized_query_hash) AS shapes
             FROM system.query_log
             WHERE event_date >= yesterday() AND is_initial_query = 0 AND has(databases, currentDatabase())
               AND has(tables, currentDatabase() || '.t05288_view_src')
               AND initial_query_id IN (SELECT query_id FROM system.query_log
                                        WHERE event_date >= yesterday() AND is_initial_query = 1 AND is_internal = 0
                                          AND current_database = currentDatabase())
             GROUP BY initial_query_id));

SELECT 'view_skips_recorded',
       (SELECT ifNull(sum(value), 0) FROM system.events
        WHERE event = 'ASTFuzzerSkippedCollaborativeWorker')
     - (SELECT workers FROM t05288_events WHERE label = 'skips_before_view') > 0;

SELECT 'view_no_internal_workers',
       (SELECT count() FROM system.query_log
        WHERE event_date >= yesterday() AND is_initial_query = 0 AND is_internal = 1
          AND has(databases, currentDatabase())
          AND has(tables, currentDatabase() || '.t05288_view_src')
          AND initial_query_id IN (SELECT query_id FROM system.query_log
                                   WHERE event_date >= yesterday() AND is_initial_query = 1 AND is_internal = 0
                                     AND current_database = currentDatabase())) = 0;

-- Server is alive after every arm above.
SELECT 'alive';

DROP VIEW t05288_view;
DROP TABLE t05288_events;
DROP TABLE t05288_dst;
DROP TABLE t05288_view_src;
DROP TABLE t05288_declared_src;
DROP TABLE t05288_worker_src;
