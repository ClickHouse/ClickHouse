-- Tags: no-ordinary-database, no-parallel
-- no-parallel: reads the server-global ProfileEvents counters
-- ASTFuzzerSkippedSharedNonParallelTarget, ASTFuzzerSkipCheckFailed and ASTFuzzerQueries, so no
-- other test may run fuzzed queries against the same server while this one measures the deltas.
-- The counters are bumped in the query-finish callback, after the query's own ProfileEvents
-- snapshot is taken, so system.query_log carries none of them and per-query_id attribution
-- (the 04339 pattern, which does not need the tag) is not available here.

-- The serverfuzz/stress profile sets ast_fuzzer_runs server-wide, which would make every statement
-- here fire the fuzzer and pollute the counters. Pin the baseline to 0 so only statements with an
-- explicit SETTINGS ast_fuzzer_runs > 0 fire it.
SET ast_fuzzer_runs = 0;
SET ast_fuzzer_any_query = 0;
SET send_logs_level = 'fatal';

DROP TABLE IF EXISTS fuzz_src;
DROP TABLE IF EXISTS fuzz_log;
DROP TABLE IF EXISTS fuzz_mt;
DROP TABLE IF EXISTS fuzz_events;

CREATE TABLE fuzz_events (label String, skipped Int64, undecided Int64, executed Int64) ENGINE = Memory;

-- sumIf over the (possibly absent) rows yields 0 before an event has ever fired, so the delta
-- arithmetic below is well defined on a fresh server. One scan keeps the three values consistent.
CREATE TABLE fuzz_src (k Int) ENGINE = Null;
CREATE TABLE fuzz_log (k Int) ENGINE = TinyLog;

INSERT INTO fuzz_events
SELECT 'before',
       toInt64(sumIf(value, event = 'ASTFuzzerSkippedSharedNonParallelTarget')),
       toInt64(sumIf(value, event = 'ASTFuzzerSkipCheckFailed')),
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
       toInt64(sumIf(value, event = 'ASTFuzzerSkipCheckFailed')),
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
       toInt64(sumIf(value, event = 'ASTFuzzerSkipCheckFailed')),
       toInt64(sumIf(value, event = 'ASTFuzzerQueries'))
FROM system.events;

SELECT 'safe_target_not_skipped',
      (SELECT skipped FROM fuzz_events WHERE label = 'after_safe')
    - (SELECT skipped FROM fuzz_events WHERE label = 'after_hazard') = 0;

SELECT 'safe_target_executed',
      (SELECT executed FROM fuzz_events WHERE label = 'after_safe')
    - (SELECT executed FROM fuzz_events WHERE label = 'after_hazard') > 0;

DROP TABLE fuzz_mv_mt;
DROP TABLE fuzz_mv;
DROP TABLE fuzz_mt;
DROP TABLE fuzz_log;
DROP TABLE fuzz_src;
DROP TABLE fuzz_events;

-- The hazard reached through a lazily loaded table. DETACH/ATTACH DATABASE replaces every plain
-- table with a proxy that renames the storage it wraps to the proxy's own id, so a walk that
-- re-entered by id would read the hop as a cycle and report the whole graph as safe.
DROP DATABASE IF EXISTS 04876_lazy;
CREATE DATABASE 04876_lazy ENGINE = Atomic SETTINGS lazy_load_tables = 1;
CREATE TABLE 04876_lazy.src (k Int) ENGINE = Null;
CREATE TABLE 04876_lazy.tgt (k Int) ENGINE = TinyLog;
DETACH DATABASE 04876_lazy;
ATTACH DATABASE 04876_lazy;
SELECT 'lazy_proxy_engine', engine FROM system.tables WHERE database = '04876_lazy' AND name = 'tgt';

CREATE TABLE fuzz_events (label String, skipped Int64, undecided Int64, executed Int64) ENGINE = Memory;
INSERT INTO fuzz_events
SELECT 'before_lazy',
       toInt64(sumIf(value, event = 'ASTFuzzerSkippedSharedNonParallelTarget')),
       toInt64(sumIf(value, event = 'ASTFuzzerSkipCheckFailed')),
       toInt64(sumIf(value, event = 'ASTFuzzerQueries'))
FROM system.events;

CREATE MATERIALIZED VIEW 04876_lazy.mv TO 04876_lazy.tgt AS SELECT k FROM 04876_lazy.src
SETTINGS ast_fuzzer_runs = 30, ast_fuzzer_any_query = 1;

INSERT INTO fuzz_events
SELECT 'after_lazy',
       toInt64(sumIf(value, event = 'ASTFuzzerSkippedSharedNonParallelTarget')),
       toInt64(sumIf(value, event = 'ASTFuzzerSkipCheckFailed')),
       toInt64(sumIf(value, event = 'ASTFuzzerQueries'))
FROM system.events;

SELECT 'lazy_proxy_clone_skipped',
      (SELECT skipped FROM fuzz_events WHERE label = 'after_lazy')
    - (SELECT skipped FROM fuzz_events WHERE label = 'before_lazy') > 0;

INSERT INTO 04876_lazy.src SETTINGS lock_acquire_timeout = 5, wait_for_async_insert_timeout = 10 VALUES (1);
SELECT 'lazy_proxy_insert_completed', count() FROM 04876_lazy.tgt;

DROP DATABASE 04876_lazy;
DROP TABLE fuzz_events;

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

CREATE TABLE fuzz_events (label String, skipped Int64, undecided Int64, executed Int64) ENGINE = Memory;
INSERT INTO fuzz_events
SELECT 'before_undecided',
       toInt64(sumIf(value, event = 'ASTFuzzerSkippedSharedNonParallelTarget')),
       toInt64(sumIf(value, event = 'ASTFuzzerSkipCheckFailed')),
       toInt64(sumIf(value, event = 'ASTFuzzerQueries'))
FROM system.events;

CREATE MATERIALIZED VIEW und_mv TO und_mt AS SELECT k FROM und_src
SETTINGS ast_fuzzer_runs = 30, ast_fuzzer_any_query = 1;

INSERT INTO fuzz_events
SELECT 'after_undecided',
       toInt64(sumIf(value, event = 'ASTFuzzerSkippedSharedNonParallelTarget')),
       toInt64(sumIf(value, event = 'ASTFuzzerSkipCheckFailed')),
       toInt64(sumIf(value, event = 'ASTFuzzerQueries'))
FROM system.events;

-- Every fuzzed CREATE here hits the unresolvable branch, so each executed query must contribute an
-- undecided verdict. Compared against the executed count rather than against zero: this same
-- counter also fires when the dependency extractor throws on a shape the fuzzer injected, which
-- happens on a few runs regardless, so a `> 0` assertion would hold even with the exits uncounted.
SELECT 'undecided_counted',
      (SELECT undecided FROM fuzz_events WHERE label = 'after_undecided')
    - (SELECT undecided FROM fuzz_events WHERE label = 'before_undecided')
   >= (SELECT executed FROM fuzz_events WHERE label = 'after_undecided')
    - (SELECT executed FROM fuzz_events WHERE label = 'before_undecided');

SELECT 'undecided_not_skipped',
      (SELECT skipped FROM fuzz_events WHERE label = 'after_undecided')
    - (SELECT skipped FROM fuzz_events WHERE label = 'before_undecided') = 0;

ATTACH TABLE und_gone;
DROP TABLE und_mv;
DROP TABLE und_mv_gone;
DROP TABLE und_gone;
DROP TABLE und_mt;
DROP TABLE und_src;
DROP TABLE fuzz_events;

SELECT 'alive';
