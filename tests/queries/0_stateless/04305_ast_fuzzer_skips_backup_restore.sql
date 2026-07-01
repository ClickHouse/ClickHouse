-- Tags: no-ordinary-database, no-parallel
-- no-parallel: reads the server-global ProfileEvents counter ASTFuzzerQueries, so no other
-- test may run fuzzed queries against the same server while this one measures the delta.

-- The serverfuzz/stress profile sets ast_fuzzer_runs server-wide, which would make every
-- statement here fire the fuzzer and pollute the ASTFuzzerQueries counter we measure. Pin the
-- baseline to 0 so only statements with an explicit SETTINGS ast_fuzzer_runs > 0 fire it.
SET ast_fuzzer_runs = 0;
SET ast_fuzzer_any_query = 0;
SET send_logs_level = 'fatal';

DROP TABLE IF EXISTS mt_fuzz_backup;
DROP TABLE IF EXISTS mt_fuzz_backup_restored;
DROP TABLE IF EXISTS fuzz_events;

CREATE TABLE mt_fuzz_backup (n Int64) ENGINE = MergeTree ORDER BY n;
INSERT INTO mt_fuzz_backup VALUES (1), (2), (3);

-- Snapshot the global counter. sum() over the (possibly absent) row yields 0 before the
-- event has ever fired, so the delta arithmetic below is well defined on a fresh server.
CREATE TABLE fuzz_events (label String, cnt Int64) ENGINE = Memory;
INSERT INTO fuzz_events SELECT 'before', toInt64(sum(value)) FROM system.events WHERE event = 'ASTFuzzerQueries';

-- executeASTFuzzerQueries runs in the query finish callback and re-executes mutated copies of
-- the just-finished query, incrementing ProfileEvents::ASTFuzzerQueries once per fuzzed query
-- it actually runs. A fuzzed BACKUP/RESTORE can start async work (BackupsWorker keeps the query
-- context alive for its background workers), and the fuzzer's per-iteration cleanup would then
-- mutate that escaped context without holding Context::mutex, racing with Context::createCopy in
-- the restore workers. The fuzzer now skips BACKUP/RESTORE with an early continue that runs
-- before the ASTFuzzerQueries increment, so none of the statements below may bump the counter.
BACKUP TABLE mt_fuzz_backup TO Memory('04305_backup') SETTINGS ast_fuzzer_runs = 3, ast_fuzzer_any_query = 1 FORMAT Null;
RESTORE TABLE mt_fuzz_backup AS mt_fuzz_backup_restored FROM Memory('04305_backup') SETTINGS ast_fuzzer_runs = 3, ast_fuzzer_any_query = 1 FORMAT Null;
-- The async variant escapes the context to background workers; the fuzzer must skip it too.
BACKUP TABLE mt_fuzz_backup TO Memory('04305_backup_async') SETTINGS async = 1, ast_fuzzer_runs = 3, ast_fuzzer_any_query = 1 FORMAT Null;

INSERT INTO fuzz_events SELECT 'after_backup', toInt64(sum(value)) FROM system.events WHERE event = 'ASTFuzzerQueries';

-- Zero delta proves the BACKUP/RESTORE queries were skipped before the counter increment. If the
-- guard in executeQuery.cpp is removed, the fuzzer runs these queries and the delta becomes > 0
-- (or the server crashes/hangs on the reintroduced race), so this is a deterministic regression
-- signal, not just a liveness check.
SELECT 'backup_restore_delta_zero',
      (SELECT cnt FROM fuzz_events WHERE label = 'after_backup')
    - (SELECT cnt FROM fuzz_events WHERE label = 'before') = 0;

-- Positive control: a plain fuzzable query is NOT skipped, so the counter must advance. This
-- proves the counter is live under these settings, so the zero delta above is a real skip and
-- not a dead/never-incremented counter giving a false pass.
SELECT 1 SETTINGS ast_fuzzer_runs = 3, ast_fuzzer_any_query = 1 FORMAT Null;

INSERT INTO fuzz_events SELECT 'after_select', toInt64(sum(value)) FROM system.events WHERE event = 'ASTFuzzerQueries';

SELECT 'plain_query_delta_positive',
      (SELECT cnt FROM fuzz_events WHERE label = 'after_select')
    - (SELECT cnt FROM fuzz_events WHERE label = 'after_backup') > 0;

-- Server is alive and the restored table holds the original rows.
SELECT 'alive', arraySort(groupArray(n)) FROM mt_fuzz_backup_restored;

DROP TABLE mt_fuzz_backup_restored;
DROP TABLE mt_fuzz_backup;
DROP TABLE fuzz_events;
