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
-- failed 15 of 15 runs, so those are bounded by this section's own executed delta instead and then
-- held 30 of 30. The undecided-versus-executed comparison remains same-window by nature: a foreign
-- decided query inflates only the executed side, so it too relies on the tag.
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

DROP TABLE IF EXISTS fuzz_src;
DROP TABLE IF EXISTS fuzz_log;
DROP TABLE IF EXISTS fuzz_mt;
DROP TABLE IF EXISTS fuzz_events;

-- One table for every section: each row is keyed by its own label, and creating it once keeps the
-- fixture independent of whether a DROP in between was honoured.
CREATE TABLE fuzz_events (label String, skipped Int64, undecided Int64, executed Int64) ENGINE = Memory;

-- sumIf over the (possibly absent) rows yields 0 before an event has ever fired, so the delta
-- arithmetic below is well defined on a fresh server. One scan keeps the three values consistent.
CREATE TABLE fuzz_src (k Int) ENGINE = Null;
CREATE TABLE fuzz_log (k Int) ENGINE = TinyLog;

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
CREATE MATERIALIZED VIEW fuzz_mv TO fuzz_log AS SELECT k FROM fuzz_src
SETTINGS ast_fuzzer_runs = 30, ast_fuzzer_any_query = 1;

INSERT INTO fuzz_events
SELECT 'after_hazard',
       toInt64(sumIf(value, event = 'ASTFuzzerSkippedSharedNonParallelTarget')),
       toInt64(sumIf(value, event = 'ASTFuzzerSharedNonParallelTargetCheckUndecided')),
       toInt64(sumIf(value, event = 'ASTFuzzerQueries'))
FROM system.events;

SELECT 'hazardous_clone_skipped',
      (SELECT skipped FROM fuzz_events WHERE label = 'after_hazard')
    - (SELECT skipped FROM fuzz_events WHERE label = 'before') > 0;

-- The counter and the wedge are separate claims: assert the INSERT itself completes. Without the
-- skip it waits out lock_acquire_timeout on the target instead of returning, so pin that timeout
-- (and the async-insert wait it feeds) low enough that a regression fails the test quickly.
INSERT INTO fuzz_src SETTINGS lock_acquire_timeout = 5, wait_for_async_insert_timeout = 10 VALUES (1);
SELECT 'insert_completed', count() FROM fuzz_log;

-- Positive control: two views on one MergeTree target is supported and must keep being fuzzed, so
-- the executed counter advances while the skip counter stays put. This also proves both counters
-- are live under these settings, so the assertion above is not reading a dead counter.
CREATE TABLE fuzz_mt (k Int) ENGINE = MergeTree ORDER BY k;
CREATE MATERIALIZED VIEW fuzz_mv_mt TO fuzz_mt AS SELECT k FROM fuzz_src
SETTINGS ast_fuzzer_runs = 30, ast_fuzzer_any_query = 1;

INSERT INTO fuzz_events
SELECT 'after_safe',
       toInt64(sumIf(value, event = 'ASTFuzzerSkippedSharedNonParallelTarget')),
       toInt64(sumIf(value, event = 'ASTFuzzerSharedNonParallelTargetCheckUndecided')),
       toInt64(sumIf(value, event = 'ASTFuzzerQueries'))
FROM system.events;

-- Bounded rather than pinned to zero: `no-parallel` only serializes within one clickhouse-test
-- process, while the stress runner drives several against one server, and a foreign fuzzed query can
-- only inflate these server-global counters. A skip delta below the executed delta still shows this
-- section's own queries were executed rather than withdrawn, which is the claim.
SELECT 'safe_target_not_skipped',
      (SELECT skipped FROM fuzz_events WHERE label = 'after_safe')
    - (SELECT skipped FROM fuzz_events WHERE label = 'after_hazard')
    < (SELECT executed FROM fuzz_events WHERE label = 'after_safe')
    - (SELECT executed FROM fuzz_events WHERE label = 'after_hazard');

SELECT 'safe_target_executed',
      (SELECT executed FROM fuzz_events WHERE label = 'after_safe')
    - (SELECT executed FROM fuzz_events WHERE label = 'after_hazard') > 0;

DROP TABLE fuzz_mv_mt;
DROP TABLE fuzz_mv;
DROP TABLE fuzz_mt;
DROP TABLE fuzz_log;
DROP TABLE fuzz_src;

-- The hazard reached through a lazily loaded table. DETACH/ATTACH DATABASE replaces every plain
-- table with a proxy that renames the storage it wraps to the proxy's own id, so a walk that
-- re-entered by id would read the hop as a cycle and report the whole graph as safe.
DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Atomic SETTINGS lazy_load_tables = 1;
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.src (k Int) ENGINE = Null;
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.tgt (k Int) ENGINE = TinyLog;
DETACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
ATTACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
SELECT 'lazy_proxy_engine', engine FROM system.tables
WHERE database = {CLICKHOUSE_DATABASE_1:String} AND name = 'tgt';

INSERT INTO fuzz_events
SELECT 'before_lazy',
       toInt64(sumIf(value, event = 'ASTFuzzerSkippedSharedNonParallelTarget')),
       toInt64(sumIf(value, event = 'ASTFuzzerSharedNonParallelTargetCheckUndecided')),
       toInt64(sumIf(value, event = 'ASTFuzzerQueries'))
FROM system.events;

CREATE MATERIALIZED VIEW {CLICKHOUSE_DATABASE_1:Identifier}.mv
TO {CLICKHOUSE_DATABASE_1:Identifier}.tgt
AS SELECT k FROM {CLICKHOUSE_DATABASE_1:Identifier}.src
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

INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.src
SETTINGS lock_acquire_timeout = 5, wait_for_async_insert_timeout = 10 VALUES (1);
SELECT 'lazy_proxy_insert_completed', count() FROM {CLICKHOUSE_DATABASE_1:Identifier}.tgt;

DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};

-- An undecided answer is counted rather than silently read as safe: the dependent view's own
-- target is detached, so that branch cannot be resolved, and the query is still fuzzed.
DROP TABLE IF EXISTS und_src;
DROP TABLE IF EXISTS und_mt;
DROP TABLE IF EXISTS und_gone;

CREATE TABLE und_src (k Int) ENGINE = Null;
CREATE TABLE und_mt (k Int) ENGINE = MergeTree ORDER BY k;
CREATE TABLE und_gone (k Int) ENGINE = TinyLog;
CREATE MATERIALIZED VIEW und_mv_gone TO und_gone AS SELECT k FROM und_mt;
DETACH TABLE und_gone;

INSERT INTO fuzz_events
SELECT 'before_undecided',
       toInt64(sumIf(value, event = 'ASTFuzzerSkippedSharedNonParallelTarget')),
       toInt64(sumIf(value, event = 'ASTFuzzerSharedNonParallelTargetCheckUndecided')),
       toInt64(sumIf(value, event = 'ASTFuzzerQueries'))
FROM system.events;

CREATE MATERIALIZED VIEW und_mv TO und_mt AS SELECT k FROM und_src
SETTINGS ast_fuzzer_runs = 30, ast_fuzzer_any_query = 1;

INSERT INTO fuzz_events
SELECT 'after_undecided',
       toInt64(sumIf(value, event = 'ASTFuzzerSkippedSharedNonParallelTarget')),
       toInt64(sumIf(value, event = 'ASTFuzzerSharedNonParallelTargetCheckUndecided')),
       toInt64(sumIf(value, event = 'ASTFuzzerQueries'))
FROM system.events;

-- Every fuzzed CREATE here hits the unresolvable branch, so each executed query must contribute an
-- undecided verdict. Compared against the executed count rather than against zero: this same counter
-- also fires when the dependency extractor throws on a shape the fuzzer injected, which happens on a
-- few runs regardless, so a `> 0` assertion would hold even with the exits uncounted. Not a fixed
-- floor either: the fuzz loop stops early on some runs (both deltas measured 30 on 18 of 20 runs and
-- 14 on the other two), and it is their equality that is the claim.
SELECT 'undecided_counted',
      (SELECT undecided FROM fuzz_events WHERE label = 'after_undecided')
    - (SELECT undecided FROM fuzz_events WHERE label = 'before_undecided')
   >= (SELECT executed FROM fuzz_events WHERE label = 'after_undecided')
    - (SELECT executed FROM fuzz_events WHERE label = 'before_undecided');

-- Lower bound for the comparison above, which two zeroes would otherwise satisfy without any
-- query having been fuzzed at all.
SELECT 'undecided_executed_nonzero',
      (SELECT executed FROM fuzz_events WHERE label = 'after_undecided')
    - (SELECT executed FROM fuzz_events WHERE label = 'before_undecided') > 0;

-- Bounded, not pinned to zero, for the same reason as safe_target_not_skipped above.
SELECT 'undecided_not_skipped',
      (SELECT skipped FROM fuzz_events WHERE label = 'after_undecided')
    - (SELECT skipped FROM fuzz_events WHERE label = 'before_undecided')
    < (SELECT executed FROM fuzz_events WHERE label = 'after_undecided')
    - (SELECT executed FROM fuzz_events WHERE label = 'before_undecided');

ATTACH TABLE und_gone;
DROP TABLE und_mv;
DROP TABLE und_mv_gone;
DROP TABLE und_gone;
DROP TABLE und_mt;
DROP TABLE und_src;

-- A queue engine sets `noPushingToViewsOnInserts`, so a direct INSERT into it pushes to no view -
-- but its background consumer inserts with `no_destination`, which does build a sink for every
-- dependent view. The dependent views of such a source are therefore walked like any other: a
-- clone sharing the TinyLog target has to be skipped. Kafka is used because it is the queue
-- carrier constructible without a live broker; the same override is on Kafka2, FileLog, NATS and
-- RabbitMQ. The skip is decided at CREATE time, so no broker is needed here.
DROP TABLE IF EXISTS q_src;
DROP TABLE IF EXISTS q_log;
DROP TABLE IF EXISTS q_mt;

CREATE TABLE q_log (k Int) ENGINE = TinyLog;
CREATE TABLE q_src (k Int) ENGINE = Kafka
    SETTINGS kafka_broker_list = 'localhost:9092', kafka_topic_list = '04876_q',
             kafka_group_name = '04876_q', kafka_format = 'CSV';

INSERT INTO fuzz_events
SELECT 'before_queue',
       toInt64(sumIf(value, event = 'ASTFuzzerSkippedSharedNonParallelTarget')),
       toInt64(sumIf(value, event = 'ASTFuzzerSharedNonParallelTargetCheckUndecided')),
       toInt64(sumIf(value, event = 'ASTFuzzerQueries'))
FROM system.events;

CREATE MATERIALIZED VIEW q_mv TO q_log AS SELECT k FROM q_src
SETTINGS ast_fuzzer_runs = 30, ast_fuzzer_any_query = 1;

INSERT INTO fuzz_events
SELECT 'after_queue',
       toInt64(sumIf(value, event = 'ASTFuzzerSkippedSharedNonParallelTarget')),
       toInt64(sumIf(value, event = 'ASTFuzzerSharedNonParallelTargetCheckUndecided')),
       toInt64(sumIf(value, event = 'ASTFuzzerQueries'))
FROM system.events;

SELECT 'queue_source_clone_skipped',
      (SELECT skipped FROM fuzz_events WHERE label = 'after_queue')
    - (SELECT skipped FROM fuzz_events WHERE label = 'before_queue') > 0;

-- Control: the same queue source over a parallel-capable target must keep being fuzzed, so a
-- positive above is not every queue-sourced view being reported hazardous.
CREATE TABLE q_mt (k Int) ENGINE = MergeTree ORDER BY k;
CREATE MATERIALIZED VIEW q_mv_mt TO q_mt AS SELECT k FROM q_src
SETTINGS ast_fuzzer_runs = 30, ast_fuzzer_any_query = 1;

INSERT INTO fuzz_events
SELECT 'after_queue_safe',
       toInt64(sumIf(value, event = 'ASTFuzzerSkippedSharedNonParallelTarget')),
       toInt64(sumIf(value, event = 'ASTFuzzerSharedNonParallelTargetCheckUndecided')),
       toInt64(sumIf(value, event = 'ASTFuzzerQueries'))
FROM system.events;

SELECT 'queue_source_safe_target_not_skipped',
      (SELECT skipped FROM fuzz_events WHERE label = 'after_queue_safe')
    - (SELECT skipped FROM fuzz_events WHERE label = 'after_queue')
    < (SELECT executed FROM fuzz_events WHERE label = 'after_queue_safe')
    - (SELECT executed FROM fuzz_events WHERE label = 'after_queue');

SELECT 'queue_source_safe_target_executed',
      (SELECT executed FROM fuzz_events WHERE label = 'after_queue_safe')
    - (SELECT executed FROM fuzz_events WHERE label = 'after_queue') > 0;

DROP TABLE q_mv_mt;
DROP TABLE q_mv;
DROP TABLE q_mt;
DROP TABLE q_src;
DROP TABLE q_log;

DROP TABLE fuzz_events;

SELECT 'alive';
