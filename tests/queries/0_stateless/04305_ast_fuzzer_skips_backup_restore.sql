-- Tags: no-ordinary-database, no-parallel

SET send_logs_level = 'fatal';

DROP TABLE IF EXISTS mt_fuzz_backup;
CREATE TABLE mt_fuzz_backup (n Int64) ENGINE = MergeTree ORDER BY n;
INSERT INTO mt_fuzz_backup VALUES (1), (2), (3);

-- `executeASTFuzzerQueries` runs in the query finish callback and re-executes mutated
-- copies of the just-finished query. A fuzzed `BACKUP`/`RESTORE` can start async work
-- (`BackupsWorker` keeps the query context alive for its background workers), and the
-- fuzzer's per-iteration cleanup would then mutate that escaped context without holding
-- `Context::mutex`, racing with `Context::createCopy` in the restore workers. The fuzzer
-- now skips `BACKUP`/`RESTORE` queries entirely, so these must complete normally and the
-- server must stay responsive.
BACKUP TABLE mt_fuzz_backup TO Memory('04305_backup') SETTINGS ast_fuzzer_runs = 3, ast_fuzzer_any_query = 1 FORMAT Null;

RESTORE TABLE mt_fuzz_backup AS mt_fuzz_backup_restored FROM Memory('04305_backup') SETTINGS ast_fuzzer_runs = 3, ast_fuzzer_any_query = 1 FORMAT Null;

-- The async variants escape the context to background workers; the fuzzer must skip them too.
BACKUP TABLE mt_fuzz_backup TO Memory('04305_backup_async') SETTINGS async = 1, ast_fuzzer_runs = 3, ast_fuzzer_any_query = 1 FORMAT Null;

-- Server is alive and the restored table holds the original rows.
SELECT 'alive', arraySort(groupArray(n)) FROM mt_fuzz_backup_restored;

DROP TABLE mt_fuzz_backup_restored;
DROP TABLE mt_fuzz_backup;
