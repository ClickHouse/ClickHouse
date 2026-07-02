-- Tags: shard

-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/71288
-- Before the fix (~24.9), querying a `Memory`-engine `DateTime64(3)` column with
-- value `0` (i.e. epoch 1970-01-01 00:00:00.000) via `remote('127.0.0.{1,2}', ...)`
-- failed with `CANNOT_PARSE_DATETIME: while converting '0' to DateTime64(3)` on
-- the analyzer path. The non-analyzer path returned the correct count.
--
-- Both paths must now return `2` (one row per shard).
-- `session_timezone='UTC'` is pinned so the `DateTime` literal interpretation is
-- stable under CI's randomized session timezone — the bug being tested is
-- independent of timezone.
-- Probable duplicate of https://github.com/ClickHouse/ClickHouse/issues/66773.

DROP TABLE IF EXISTS test_71288;
CREATE TABLE test_71288 (r DateTime64(3)) ENGINE = Memory() AS SELECT 0;

SELECT count()
FROM remote('127.0.0.{1,2}', currentDatabase(), test_71288)
WHERE r IN (toDateTime64('1970-01-01 00:00:00.000', 3))
SETTINGS allow_experimental_analyzer = 0, session_timezone = 'UTC';

SELECT count()
FROM remote('127.0.0.{1,2}', currentDatabase(), test_71288)
WHERE r IN (toDateTime64('1970-01-01 00:00:00.000', 3))
SETTINGS allow_experimental_analyzer = 1, session_timezone = 'UTC';

DROP TABLE test_71288;
