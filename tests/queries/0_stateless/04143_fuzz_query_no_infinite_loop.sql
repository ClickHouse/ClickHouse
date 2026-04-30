-- Tags: no-fasttest, no-parallel

-- Regression test for `fuzzQuery` infinite loop in `FuzzQuerySource::createColumn`.
-- The previous code reset `fuzz_base` on every iteration when `max_query_length > 500`,
-- never producing a row, so any `SELECT FROM fuzzQuery(..., > 500)` hung forever.
-- The same shape can occur after tightening the cap if every fuzzed AST exceeds the
-- limit. The source must always make progress: cap retries per row and fall back to the
-- original query when fuzzing cannot fit the cap.

-- Cap above 500: with the old check `if (config.max_query_length > 500)` this hangs.
SELECT count() AS rows_returned FROM (
    SELECT * FROM fuzzQuery('SELECT 1', 1000, 42) LIMIT 5
);

-- Cap right at the threshold.
SELECT count() AS rows_returned FROM (
    SELECT * FROM fuzzQuery('SELECT 1', 501, 42) LIMIT 5
);

-- Tiny cap below the unmutated base query's formatted length: no fuzzed variant can
-- possibly satisfy the cap. Falling back to the unfuzzed query would emit oversized
-- rows and silently violate the documented `max_query_length` contract; reject the
-- impossible configuration up front instead.
SELECT * FROM fuzzQuery('SELECT 1 + 2 + 3 + 4 + 5 + 6 + 7 + 8 + 9 + 10', 1, 42) LIMIT 5; -- { serverError BAD_ARGUMENTS }

-- `max_query_length = 0` is pathological: every non-empty fuzzed AST exceeds the cap.
-- Reject it at configuration time rather than degrading to a no-op fuzzer.
SELECT * FROM fuzzQuery('SELECT 1', 0, 42) LIMIT 1; -- { serverError BAD_ARGUMENTS }

-- Same rejections via the storage engine path on fresh `CREATE TABLE`.
DROP TABLE IF EXISTS fq_t;
CREATE TABLE fq_t (q String) ENGINE = FuzzQuery('SELECT 1 + 2 + 3 + 4 + 5 + 6 + 7 + 8 + 9 + 10', 1, 42); -- { serverError BAD_ARGUMENTS }
CREATE TABLE fq_t (q String) ENGINE = FuzzQuery('SELECT 1', 0, 42); -- { serverError BAD_ARGUMENTS }

-- Backward compatibility: an existing FuzzQuery table with a previously-tolerated config
-- (cap below the base query's formatted size, or `max_query_length = 0`) must still
-- attach. Validation only fires on `CREATE` / `SECONDARY_CREATE`; on `ATTACH` and
-- stronger we keep the legacy lenient behavior so server startup / metadata load on an
-- upgraded server does not reject existing tables. The `FuzzQuerySource` loop is already
-- safe at runtime — it caps retries per row and falls back to the unfuzzed query — so
-- the table is still usable.
--
-- We need an `Ordinary` database so `ATTACH TABLE name (cols) ENGINE = ...` does not
-- require a pre-existing UUID (full ATTACH only works that way in `Atomic`). A custom
-- database also avoids polluting the test's default `Atomic` database.
SET send_logs_level = 'fatal';
DROP DATABASE IF EXISTS test_04143_attach;
SET allow_deprecated_database_ordinary = 1;
CREATE DATABASE test_04143_attach ENGINE = Ordinary;

ATTACH TABLE test_04143_attach.fq_attach_tiny (q String)
    ENGINE = FuzzQuery('SELECT 1 + 2 + 3 + 4 + 5 + 6 + 7 + 8 + 9 + 10', 1, 42);
SELECT 'attach with tiny cap succeeded';

ATTACH TABLE test_04143_attach.fq_attach_zero (q String)
    ENGINE = FuzzQuery('SELECT 1', 0, 42);
SELECT 'attach with zero cap succeeded';

DROP DATABASE test_04143_attach;
