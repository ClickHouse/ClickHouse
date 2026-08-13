-- Tags: no-ordinary-database, no-parallel, log-engine, no-replicated-database, no-fasttest
-- no-fasttest: the queue-source section below needs the Kafka engine, which the fast-test build
-- does not have.
-- no-parallel: reads the server-global ProfileEvents counters
-- ASTFuzzerSkippedSharedNonParallelTarget, ASTFuzzerSharedNonParallelTargetCheckUndecided and
-- ASTFuzzerQueries, so no other test may run fuzzed queries against the same server while this one
-- measures the deltas.
-- The counters are bumped in the query-finish callback, after the query's own ProfileEvents
-- snapshot is taken, so system.query_log carries none of them and per-query_id attribution
-- (the 04339 pattern, which does not need the tag) is not available here.
-- The tag is what makes the deltas attributable, and it is load-bearing rather than cosmetic: it
-- serializes only within one clickhouse-test process, while the stress runner drives several against
-- one server. Measured under a concurrent fuzzing process, every claim that a delta was exactly zero
-- failed 15 of 15 runs, so those are bounded by this section's own executed delta instead.
-- log-engine: the whole oracle rests on the target NOT supporting parallel insert, which
-- --replace-log-memory-with-mergetree rewrites away (it also rewrites the Memory fixture).
-- no-replicated-database: for the lazy_load_tables section below, plus a DETACH TABLE inside the
-- test database.

-- The serverfuzz/stress profile sets ast_fuzzer_runs server-wide, which would make every statement
-- here fire the fuzzer and pollute the counters. Pin the baseline to 0 so only statements with an
-- explicit SETTINGS ast_fuzzer_runs > 0 fire it.
SET ast_fuzzer_runs = 0;
SET ast_fuzzer_any_query = 0;
SET send_logs_level = 'fatal';
-- The stress runner appends this unconditionally; a DROP of a table that stores no data on disk
-- then becomes a TRUNCATE, so fuzz_events would survive its own DROP and the next CREATE would fail.
SET ignore_drop_queries_probability = 0;

-- Every fixture a fuzzed statement can touch lives in a SECOND database that this file drops and
-- recreates up front. Two reasons, both measured:
--   * a clone survives its own statement (`fuzz_mv__fuzz_29`, `q_mv_mt__fuzz_16` ...) and no fixed
--     DROP list can name it; 8-17 such views were left behind by a single clean pass. They are
--     created in the database of the VIEW, not of the connection, so dropping that database removes
--     all of them at once.
--   * a leftover clone whose TO target is later dropped turns the next run's INSERT into
--     `Code: 60 UNKNOWN_TABLE`. With a fixed --database the runner neither creates nor drops the
--     per-test database, so without this reset the second run in one database failed (measured:
--     first failure at run 2 of 8, in 3 of 4 trials).
-- `fuzz_events` stays in the test's own database: nothing fuzzed writes to it, and keeping the
-- counter bookkeeping outside the reset database means a reset cannot discard the deltas.
DROP TABLE IF EXISTS fuzz_events;
DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier} SYNC;
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier};

-- One table for every section: each row is keyed by its own label, and creating it once keeps the
-- fixture independent of whether a DROP in between was honoured.
CREATE TABLE fuzz_events (label String, skipped Int64, undecided Int64, executed Int64) ENGINE = Memory;

-- sumIf over the (possibly absent) rows yields 0 before an event has ever fired, so the delta
-- arithmetic below is well defined on a fresh server. One scan keeps the three values consistent.
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.fuzz_src (k Int) ENGINE = Null;
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.fuzz_log (k Int) ENGINE = TinyLog;

INSERT INTO fuzz_events
SELECT 'before',
       toInt64(sumIf(value, event = 'ASTFuzzerSkippedSharedNonParallelTarget')),
       toInt64(sumIf(value, event = 'ASTFuzzerSharedNonParallelTargetCheckUndecided')),
       toInt64(sumIf(value, event = 'ASTFuzzerQueries'))
FROM system.events;

-- Fuzzing a CREATE MATERIALIZED VIEW renames the view but keeps its external TO, so the clone
-- shares both source and target with this view. One INSERT INTO fuzz_src would then build two
-- sinks for fuzz_log, which takes a single write() per INSERT and holds its lock from pipeline
-- build to finish: the second sink waits out lock_acquire_timeout and the INSERT never completes.
-- The clone is stochastic (most die on a name collision first), so drive enough runs and assert a
-- positive delta rather than an exact count.
-- Microsecond boundary from the SERVER, stored in the fixture's `executed` slot: every clone this
-- section logs starts after it, and every row an earlier run left behind starts before it.
INSERT INTO fuzz_events SELECT 'hazard_boundary', 0, 0, toUnixTimestamp64Micro(now64(6));

CREATE MATERIALIZED VIEW {CLICKHOUSE_DATABASE_1:Identifier}.fuzz_mv TO {CLICKHOUSE_DATABASE_1:Identifier}.fuzz_log AS SELECT k FROM {CLICKHOUSE_DATABASE_1:Identifier}.fuzz_src
SETTINGS ast_fuzzer_runs = 30, ast_fuzzer_any_query = 1, log_comment = '04876_hazard';

INSERT INTO fuzz_events
SELECT 'after_hazard',
       toInt64(sumIf(value, event = 'ASTFuzzerSkippedSharedNonParallelTarget')),
       toInt64(sumIf(value, event = 'ASTFuzzerSharedNonParallelTargetCheckUndecided')),
       toInt64(sumIf(value, event = 'ASTFuzzerQueries'))
FROM system.events;

SELECT 'hazardous_clone_skipped',
      (SELECT skipped FROM fuzz_events WHERE label = 'after_hazard')
    - (SELECT skipped FROM fuzz_events WHERE label = 'before') > 0;

SYSTEM FLUSH LOGS query_log;

INSERT INTO fuzz_events
SELECT 'hazard_own_clones', 0, 0, count(DISTINCT query_id)
FROM system.query_log
WHERE event_date >= today() - 1
  AND toUnixTimestamp64Micro(event_time_microseconds)
      >= (SELECT executed FROM fuzz_events WHERE label = 'hazard_boundary')
  AND current_database = currentDatabase()
  AND log_comment = '04876_hazard'
  AND query LIKE '%fuzz\_mv\_\_fuzz%'
SETTINGS enable_parallel_replicas = 0;

-- The counter and the wedge are separate claims: assert the INSERT itself completes. Without the
-- skip it waits out lock_acquire_timeout on the target instead of returning, so pin that timeout
-- (and the async-insert wait it feeds) low enough that a regression fails the test quickly.
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.fuzz_src SETTINGS lock_acquire_timeout = 5, wait_for_async_insert_timeout = 10 VALUES (1);
SELECT 'insert_completed', count() FROM {CLICKHOUSE_DATABASE_1:Identifier}.fuzz_log;

-- Positive control: two views on one MergeTree target is supported and must keep being fuzzed, so
-- the executed counter advances while the skip counter stays put. This also proves both counters
-- are live under these settings, so the assertion above is not reading a dead counter.
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.fuzz_mt (k Int) ENGINE = MergeTree ORDER BY k;
-- Microsecond boundary from the SERVER, stored in the fixture's `executed` slot: every clone this
-- section logs starts after it, and every row an earlier run left behind starts before it.
INSERT INTO fuzz_events SELECT 'safe_boundary', 0, 0, toUnixTimestamp64Micro(now64(6));

CREATE MATERIALIZED VIEW {CLICKHOUSE_DATABASE_1:Identifier}.fuzz_mv_mt TO {CLICKHOUSE_DATABASE_1:Identifier}.fuzz_mt AS SELECT k FROM {CLICKHOUSE_DATABASE_1:Identifier}.fuzz_src
SETTINGS ast_fuzzer_runs = 30, ast_fuzzer_any_query = 1, log_comment = '04876_safe';

INSERT INTO fuzz_events
SELECT 'after_safe',
       toInt64(sumIf(value, event = 'ASTFuzzerSkippedSharedNonParallelTarget')),
       toInt64(sumIf(value, event = 'ASTFuzzerSharedNonParallelTargetCheckUndecided')),
       toInt64(sumIf(value, event = 'ASTFuzzerQueries'))
FROM system.events;

-- Attributable to THIS fixture rather than to the server-global counters. The skip event is
-- incremented in the query-finish callback and the loop then `continue`s, while `ASTFuzzerQueries` is
-- incremented only on the execution path, so a foreign hazardous fuzz raises the skip side of a
-- relative comparison without raising the executed side and a foreign skip can satisfy it outright.
-- What is claimed here is about this section's OWN clones, and that is directly observable in
-- query_log: a withdrawn clone never reaches execution and is therefore never logged, while an
-- executed one always is. The database, the log_comment and the boundary timestamp together exclude
-- both a foreign process and an earlier run of this same file.
SYSTEM FLUSH LOGS query_log;

INSERT INTO fuzz_events
SELECT 'safe_own_clones', 0, 0, count(DISTINCT query_id)
FROM system.query_log
WHERE event_date >= today() - 1
  AND toUnixTimestamp64Micro(event_time_microseconds)
      >= (SELECT executed FROM fuzz_events WHERE label = 'safe_boundary')
  AND current_database = currentDatabase()
  AND log_comment = '04876_safe'
  AND query LIKE '%fuzz\_mv\_mt\_\_fuzz%'
SETTINGS enable_parallel_replicas = 0;

-- Executed, not withdrawn: a clone of the safe-target view reached execution.
SELECT 'safe_target_executed',
      (SELECT executed FROM fuzz_events WHERE label = 'safe_own_clones') > 0;

-- And not skipped, stated against THIS file's other section rather than a server-global delta: the
-- hazardous section withdraws almost every clone it generates while the safe section executes almost
-- all of its own, so the safe section's own logged-clone count must exceed the hazardous section's.
-- Both sides are counted from this file's own query_log rows, so a foreign fuzzing process cannot
-- move either one. Measured margin over 5 trials: hazard 1-7 of 30, safe 29-30 of 30.
SELECT 'safe_target_not_skipped',
      (SELECT executed FROM fuzz_events WHERE label = 'safe_own_clones')
    > (SELECT executed FROM fuzz_events WHERE label = 'hazard_own_clones');



-- The hazard reached through a lazily loaded table. DETACH/ATTACH DATABASE replaces every plain
-- table with a proxy that renames the storage it wraps to the proxy's own id, so a walk that
-- re-entered by id would read the hop as a cycle and report the whole graph as safe.
-- Dropped SYNC rather than plain: an Atomic database dropped asynchronously can still hold its
-- metadata directory when the CREATE below runs.
DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_2:Identifier} SYNC;
CREATE DATABASE {CLICKHOUSE_DATABASE_2:Identifier} ENGINE = Atomic SETTINGS lazy_load_tables = 1;
CREATE TABLE {CLICKHOUSE_DATABASE_2:Identifier}.src (k Int) ENGINE = Null;
CREATE TABLE {CLICKHOUSE_DATABASE_2:Identifier}.tgt (k Int) ENGINE = TinyLog;
-- The DETACH/ATTACH pair is what replaces every table with a proxy, and it is load-bearing:
-- `lazy_load_tables = 1` alone leaves `tgt` reported as `TinyLog` (measured), so the proxy hop this
-- section exists to exercise is never reached without it.
-- The two statements are adjacent so nothing that can fail sits inside the detached interval. That
-- matters because a database left detached is invisible to `DROP DATABASE IF EXISTS` -- which then
-- succeeds without removing anything while the metadata directory survives, so the next run's CREATE
-- fails with `Code: 521 Cannot rename ...` (measured, with and without SYNC), and no SQL-level
-- recovery is available: a detached database appears in neither system.databases nor
-- system.detached_tables, and `ATTACH DATABASE IF NOT EXISTS` on a never-created name throws
-- `Code: 336`. Keeping the interval empty is therefore the whole guarantee.
DETACH DATABASE {CLICKHOUSE_DATABASE_2:Identifier};
ATTACH DATABASE {CLICKHOUSE_DATABASE_2:Identifier};
SELECT 'lazy_proxy_engine', engine FROM system.tables
WHERE database = {CLICKHOUSE_DATABASE_2:String} AND name = 'tgt';

INSERT INTO fuzz_events
SELECT 'before_lazy',
       toInt64(sumIf(value, event = 'ASTFuzzerSkippedSharedNonParallelTarget')),
       toInt64(sumIf(value, event = 'ASTFuzzerSharedNonParallelTargetCheckUndecided')),
       toInt64(sumIf(value, event = 'ASTFuzzerQueries'))
FROM system.events;

CREATE MATERIALIZED VIEW {CLICKHOUSE_DATABASE_2:Identifier}.mv
TO {CLICKHOUSE_DATABASE_2:Identifier}.tgt
AS SELECT k FROM {CLICKHOUSE_DATABASE_2:Identifier}.src
SETTINGS ast_fuzzer_runs = 30, ast_fuzzer_any_query = 1;

INSERT INTO fuzz_events
SELECT 'after_lazy',
       toInt64(sumIf(value, event = 'ASTFuzzerSkippedSharedNonParallelTarget')),
       toInt64(sumIf(value, event = 'ASTFuzzerSharedNonParallelTargetCheckUndecided')),
       toInt64(sumIf(value, event = 'ASTFuzzerQueries'))
FROM system.events;

SELECT 'lazy_proxy_clone_skipped',
      (SELECT skipped FROM fuzz_events WHERE label = 'after_lazy')
    - (SELECT skipped FROM fuzz_events WHERE label = 'before_lazy') > 0;

INSERT INTO {CLICKHOUSE_DATABASE_2:Identifier}.src
SETTINGS lock_acquire_timeout = 5, wait_for_async_insert_timeout = 10 VALUES (1);
SELECT 'lazy_proxy_insert_completed', count() FROM {CLICKHOUSE_DATABASE_2:Identifier}.tgt;

DROP DATABASE {CLICKHOUSE_DATABASE_2:Identifier};

-- An undecided answer is counted rather than silently read as safe: the dependent view's own
-- target is detached, so that branch cannot be resolved, and the query is still fuzzed.
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.und_src (k Int) ENGINE = Null;
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.und_mt (k Int) ENGINE = MergeTree ORDER BY k;
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.und_gone (k Int) ENGINE = TinyLog;
CREATE MATERIALIZED VIEW {CLICKHOUSE_DATABASE_1:Identifier}.und_mv_gone TO {CLICKHOUSE_DATABASE_1:Identifier}.und_gone AS SELECT k FROM {CLICKHOUSE_DATABASE_1:Identifier}.und_mt;
-- Dropped rather than detached. Either makes the dependent view's target unresolvable, so the walk
-- reports undecided identically (measured: 30 undecided of 30 runs both ways), but a detached table
-- is invisible to DROP and its metadata file survives, so a run that failed anywhere inside the
-- detached interval left `und_gone` undroppable and the next run in the same database hit
-- `already exists (detached)`. The interval here spans a 30-run fuzz loop, which is the widest
-- window in the file; DROP leaves nothing to recover.
DROP TABLE {CLICKHOUSE_DATABASE_1:Identifier}.und_gone;

INSERT INTO fuzz_events
SELECT 'before_undecided',
       toInt64(sumIf(value, event = 'ASTFuzzerSkippedSharedNonParallelTarget')),
       toInt64(sumIf(value, event = 'ASTFuzzerSharedNonParallelTargetCheckUndecided')),
       toInt64(sumIf(value, event = 'ASTFuzzerQueries'))
FROM system.events;

-- Microsecond boundary, taken from the SERVER and stored in the fixture's `executed` slot. Every
-- clone this section logs starts after it, and every row an earlier run of this file left behind
-- starts before it.
INSERT INTO fuzz_events SELECT 'undecided_boundary', 0, 0, toUnixTimestamp64Micro(now64(6));

CREATE MATERIALIZED VIEW {CLICKHOUSE_DATABASE_1:Identifier}.und_mv TO {CLICKHOUSE_DATABASE_1:Identifier}.und_mt AS SELECT k FROM {CLICKHOUSE_DATABASE_1:Identifier}.und_src
SETTINGS ast_fuzzer_runs = 30, ast_fuzzer_any_query = 1, log_comment = '04876_undecided';

INSERT INTO fuzz_events
SELECT 'after_undecided',
       toInt64(sumIf(value, event = 'ASTFuzzerSkippedSharedNonParallelTarget')),
       toInt64(sumIf(value, event = 'ASTFuzzerSharedNonParallelTargetCheckUndecided')),
       toInt64(sumIf(value, event = 'ASTFuzzerQueries'))
FROM system.events;

-- Every fuzzed CREATE here hits the unresolvable branch, so each executed query must contribute an
-- undecided verdict. The right side counts this section's OWN clones out of query_log instead of a
-- server-global counter: a withdrawn clone never reaches execution and so is never logged, while an
-- executed one always is. A foreign fuzzing process can therefore only raise the left side, which
-- makes the inequality one-sided in the safe direction rather than same-window.
-- The database and log_comment predicates separate a foreign process; the boundary timestamp is
-- what separates an earlier run of this same file, whose rows carry the same constant comment and,
-- when the runner is given a fixed --database, the same database too.
SYSTEM FLUSH LOGS query_log;

INSERT INTO fuzz_events
SELECT 'undecided_own_clones', 0, 0, count(DISTINCT query_id)
FROM system.query_log
WHERE event_date >= today() - 1
  AND toUnixTimestamp64Micro(event_time_microseconds)
      >= (SELECT executed FROM fuzz_events WHERE label = 'undecided_boundary')
  AND current_database = currentDatabase()
  AND log_comment = '04876_undecided'
  AND query LIKE '%und\_mv\_\_fuzz%'
SETTINGS enable_parallel_replicas = 0;

SELECT 'undecided_counted',
      (SELECT undecided FROM fuzz_events WHERE label = 'after_undecided')
    - (SELECT undecided FROM fuzz_events WHERE label = 'before_undecided')
   >= (SELECT executed FROM fuzz_events WHERE label = 'undecided_own_clones');

-- Lower bound for the comparison above, which two zeroes would otherwise satisfy without any
-- query having been fuzzed at all.
SELECT 'undecided_executed_nonzero',
      (SELECT executed FROM fuzz_events WHERE label = 'undecided_own_clones') > 0;


-- A queue engine sets `noPushingToViewsOnInserts`, so a direct INSERT into it pushes to no view -
-- but its background consumer inserts with `no_destination`, which does build a sink for every
-- dependent view. The dependent views of such a source are therefore walked like any other: a
-- clone sharing the TinyLog target has to be skipped. Kafka is used because it is the queue
-- carrier constructible without a live broker; the same override is on Kafka2, FileLog, NATS and
-- RabbitMQ. The skip is decided at CREATE time, so no broker is needed here.
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.q_log (k Int) ENGINE = TinyLog;
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.q_src (k Int) ENGINE = Kafka
    SETTINGS kafka_broker_list = 'localhost:9092', kafka_topic_list = '04876_q',
             kafka_group_name = '04876_q', kafka_format = 'CSV';

INSERT INTO fuzz_events
SELECT 'before_queue',
       toInt64(sumIf(value, event = 'ASTFuzzerSkippedSharedNonParallelTarget')),
       toInt64(sumIf(value, event = 'ASTFuzzerSharedNonParallelTargetCheckUndecided')),
       toInt64(sumIf(value, event = 'ASTFuzzerQueries'))
FROM system.events;

-- Boundary for this section's own clones, as in the sections above.
INSERT INTO fuzz_events SELECT 'queue_boundary', 0, 0, toUnixTimestamp64Micro(now64(6));

CREATE MATERIALIZED VIEW {CLICKHOUSE_DATABASE_1:Identifier}.q_mv TO {CLICKHOUSE_DATABASE_1:Identifier}.q_log AS SELECT k FROM {CLICKHOUSE_DATABASE_1:Identifier}.q_src
SETTINGS ast_fuzzer_runs = 30, ast_fuzzer_any_query = 1, log_comment = '04876_queue';

INSERT INTO fuzz_events
SELECT 'after_queue',
       toInt64(sumIf(value, event = 'ASTFuzzerSkippedSharedNonParallelTarget')),
       toInt64(sumIf(value, event = 'ASTFuzzerSharedNonParallelTargetCheckUndecided')),
       toInt64(sumIf(value, event = 'ASTFuzzerQueries'))
FROM system.events;

SELECT 'queue_source_clone_skipped',
      (SELECT skipped FROM fuzz_events WHERE label = 'after_queue')
    - (SELECT skipped FROM fuzz_events WHERE label = 'before_queue') > 0;

SYSTEM FLUSH LOGS query_log;

INSERT INTO fuzz_events
SELECT 'queue_own_clones', 0, 0, count(DISTINCT query_id)
FROM system.query_log
WHERE event_date >= today() - 1
  AND toUnixTimestamp64Micro(event_time_microseconds)
      >= (SELECT executed FROM fuzz_events WHERE label = 'queue_boundary')
  AND current_database = currentDatabase()
  AND log_comment = '04876_queue'
  AND query LIKE '%q\_mv\_\_fuzz%'
SETTINGS enable_parallel_replicas = 0;

-- Control: the same queue source over a parallel-capable target must keep being fuzzed, so a
-- positive above is not every queue-sourced view being reported hazardous.
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.q_mt (k Int) ENGINE = MergeTree ORDER BY k;
-- Boundary for this section's own clones, as in the safe-target section above.
INSERT INTO fuzz_events SELECT 'queue_safe_boundary', 0, 0, toUnixTimestamp64Micro(now64(6));

CREATE MATERIALIZED VIEW {CLICKHOUSE_DATABASE_1:Identifier}.q_mv_mt TO {CLICKHOUSE_DATABASE_1:Identifier}.q_mt AS SELECT k FROM {CLICKHOUSE_DATABASE_1:Identifier}.q_src
SETTINGS ast_fuzzer_runs = 30, ast_fuzzer_any_query = 1, log_comment = '04876_qsafe';

INSERT INTO fuzz_events
SELECT 'after_queue_safe',
       toInt64(sumIf(value, event = 'ASTFuzzerSkippedSharedNonParallelTarget')),
       toInt64(sumIf(value, event = 'ASTFuzzerSharedNonParallelTargetCheckUndecided')),
       toInt64(sumIf(value, event = 'ASTFuzzerQueries'))
FROM system.events;

-- Same fixture-attributable form as the safe-target section: a foreign fuzz can only move the
-- server-global counters, never this section's own logged clones.
SYSTEM FLUSH LOGS query_log;

INSERT INTO fuzz_events
SELECT 'queue_safe_own_clones', 0, 0, count(DISTINCT query_id)
FROM system.query_log
WHERE event_date >= today() - 1
  AND toUnixTimestamp64Micro(event_time_microseconds)
      >= (SELECT executed FROM fuzz_events WHERE label = 'queue_safe_boundary')
  AND current_database = currentDatabase()
  AND log_comment = '04876_qsafe'
  AND query LIKE '%q\_mv\_mt\_\_fuzz%'
SETTINGS enable_parallel_replicas = 0;

SELECT 'queue_source_safe_target_executed',
      (SELECT executed FROM fuzz_events WHERE label = 'queue_safe_own_clones') > 0;

SELECT 'queue_source_safe_target_not_skipped',
      (SELECT executed FROM fuzz_events WHERE label = 'queue_safe_own_clones')
    > (SELECT executed FROM fuzz_events WHERE label = 'queue_own_clones');


-- One statement removes every fixture AND every fuzz clone, whatever the clones were named.
DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier} SYNC;
DROP TABLE fuzz_events;

SELECT 'alive';
