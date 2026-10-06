-- Tags: shard

-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/71288
-- Before the fix (~24.9), querying a `Memory`-engine `DateTime64(3)` column with
-- value `0` (i.e. epoch 1970-01-01 00:00:00.000) via `remote('127.0.0.{1,2}', ...)`
-- failed with `CANNOT_PARSE_DATETIME: while converting '0' to DateTime64(3)` on
-- the analyzer path. The non-analyzer path returned the correct count.
--
-- The fixture holds one matching epoch-zero row plus one non-matching row, so a
-- silently dropped remote `WHERE r IN (...)` filter would degenerate to an
-- unfiltered count and return `4` instead of `2`. That is, this test fails if the
-- filter path is lost, not only if the query errors out.
-- Both paths must now return `2` (one matching row per shard).
-- `session_timezone='UTC'` is pinned so the `DateTime` literal interpretation is
-- stable under CI's randomized session timezone; the bug being tested is
-- independent of timezone.
-- Probable duplicate of https://github.com/ClickHouse/ClickHouse/issues/66773.

DROP TABLE IF EXISTS test_71288;
CREATE TABLE test_71288 (r DateTime64(3)) ENGINE = Memory();
-- 0 = epoch zero (the value that triggered the original bug); 1000 = epoch + 1s (non-matching).
INSERT INTO test_71288 VALUES (0), (1000);

SELECT count()
FROM remote('127.0.0.{1,2}', currentDatabase(), test_71288)
WHERE r IN (toDateTime64('1970-01-01 00:00:00.000', 3))
SETTINGS allow_experimental_analyzer = 0, session_timezone = 'UTC';

SELECT count()
FROM remote('127.0.0.{1,2}', currentDatabase(), test_71288)
WHERE r IN (toDateTime64('1970-01-01 00:00:00.000', 3))
SETTINGS allow_experimental_analyzer = 1, session_timezone = 'UTC';

DROP TABLE test_71288;
