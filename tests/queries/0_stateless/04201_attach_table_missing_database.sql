-- Test: stub ATTACH TABLE on a non-existent database (without ON CLUSTER)
-- Covers: src/Interpreters/InterpreterCreateQuery.cpp:1585-1586 — defensive throw of UNKNOWN_DATABASE
--   added by PR #61365. Triggered when (create.attach && !create.storage && !create.columns_list)
--   AND `tryGetDatabase` returns null AND `create.cluster.empty()`.
--   Without this guard, control would fall through to line 1596 `database->getCreateTableQuery(...)`
--   dereferencing a null DatabasePtr.
DROP DATABASE IF EXISTS test_attach_missing_db_pr61365;
ATTACH TABLE test_attach_missing_db_pr61365.no_such_table; -- { serverError UNKNOWN_DATABASE }
ATTACH DICTIONARY test_attach_missing_db_pr61365.no_such_dict; -- { serverError UNKNOWN_DATABASE }
