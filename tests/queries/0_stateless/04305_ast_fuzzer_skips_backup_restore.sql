-- Tags: no-ordinary-database, no-parallel
-- no-parallel: reads the server-global ProfileEvents counters ASTFuzzerSkippedBackupRestore
-- and ASTFuzzerQueries, so no other test may run fuzzed queries against the same server while
-- this one measures the deltas.

-- The serverfuzz/stress profile sets ast_fuzzer_runs server-wide, which would make every
-- statement here fire the fuzzer and pollute the counters we measure. Pin the baseline to 0
-- so only statements with an explicit SETTINGS ast_fuzzer_runs > 0 fire it.
SET ast_fuzzer_runs = 0;
SET ast_fuzzer_any_query = 0;
SET send_logs_level = 'fatal';

DROP TABLE IF EXISTS mt_fuzz_backup;
DROP TABLE IF EXISTS mt_fuzz_backup_restored;
DROP TABLE IF EXISTS fuzz_events;

CREATE TABLE mt_fuzz_backup (n Int64) ENGINE = MergeTree ORDER BY n;
INSERT INTO mt_fuzz_backup VALUES (1), (2), (3);

-- Snapshot both global counters in a single scan so they are consistent. sumIf over the
-- (possibly absent) rows yields 0 before an event has ever fired, so the delta arithmetic
-- below is well defined on a fresh server.
CREATE TABLE fuzz_events (label String, skipped Int64, executed Int64) ENGINE = Memory;
INSERT INTO fuzz_events
SELECT 'before',
       toInt64(sumIf(value, event = 'ASTFuzzerSkippedBackupRestore')),
       toInt64(sumIf(value, event = 'ASTFuzzerQueries'))
FROM system.events;

-- executeASTFuzzerQueries runs in the query finish callback and re-executes mutated copies of
-- the just-finished query. A fuzzed BACKUP/RESTORE can start async work (BackupsWorker keeps the
-- query context alive for its background workers), and the fuzzer's per-iteration cleanup would
-- then mutate that escaped context without holding Context::mutex, racing with Context::createCopy
-- in the restore workers. The fuzzer skips BACKUP/RESTORE via an early continue that runs before
-- any other guard (depth/format/length) and bumps ASTFuzzerSkippedBackupRestore instead of
-- executing the query and bumping ASTFuzzerQueries.
BACKUP TABLE mt_fuzz_backup TO Memory('04305_backup') SETTINGS ast_fuzzer_runs = 3, ast_fuzzer_any_query = 1 FORMAT Null;
RESTORE TABLE mt_fuzz_backup AS mt_fuzz_backup_restored FROM Memory('04305_backup') SETTINGS ast_fuzzer_runs = 3, ast_fuzzer_any_query = 1 FORMAT Null;
-- The async variant escapes the context to background workers; the fuzzer must skip it too.
BACKUP TABLE mt_fuzz_backup TO Memory('04305_backup_async') SETTINGS async = 1, ast_fuzzer_runs = 3, ast_fuzzer_any_query = 1 FORMAT Null;

INSERT INTO fuzz_events
SELECT 'after_backup',
       toInt64(sumIf(value, event = 'ASTFuzzerSkippedBackupRestore')),
       toInt64(sumIf(value, event = 'ASTFuzzerQueries'))
FROM system.events;

-- Deterministic proof of the skip contract: the fuzzed BACKUP/RESTORE queries reached the
-- ASTBackupQuery guard and were counted as skipped. The guard is checked before the
-- depth/format/length early-continue paths, so this positive count is specific to the query
-- type and cannot be produced by those unrelated skips. If the guard in executeQuery.cpp is
-- removed, these fuzzed queries are executed instead (or the server crashes/hangs on the
-- reintroduced race), so this counter stays 0 and the assertion flips to 0.
SELECT 'backup_restore_skipped',
      (SELECT skipped FROM fuzz_events WHERE label = 'after_backup')
    - (SELECT skipped FROM fuzz_events WHERE label = 'before') > 0;

-- The skipped queries were not executed, so the executed-query counter must not advance for them.
SELECT 'backup_restore_not_executed',
      (SELECT executed FROM fuzz_events WHERE label = 'after_backup')
    - (SELECT executed FROM fuzz_events WHERE label = 'before') = 0;

-- Positive control: a plain fuzzable query is NOT a BACKUP/RESTORE, so it is executed (not
-- skipped). The executed counter must advance and the skip counter must not. This proves both
-- counters are live under these settings, so the values above are real and not dead counters.
SELECT 1 SETTINGS ast_fuzzer_runs = 3, ast_fuzzer_any_query = 1 FORMAT Null;

INSERT INTO fuzz_events
SELECT 'after_select',
       toInt64(sumIf(value, event = 'ASTFuzzerSkippedBackupRestore')),
       toInt64(sumIf(value, event = 'ASTFuzzerQueries'))
FROM system.events;

SELECT 'plain_query_executed',
      (SELECT executed FROM fuzz_events WHERE label = 'after_select')
    - (SELECT executed FROM fuzz_events WHERE label = 'after_backup') > 0;

SELECT 'plain_query_not_skipped',
      (SELECT skipped FROM fuzz_events WHERE label = 'after_select')
    - (SELECT skipped FROM fuzz_events WHERE label = 'after_backup') = 0;

-- Server is alive and the restored table holds the original rows.
SELECT 'alive', arraySort(groupArray(n)) FROM mt_fuzz_backup_restored;

DROP TABLE mt_fuzz_backup_restored;
DROP TABLE mt_fuzz_backup;
DROP TABLE fuzz_events;
