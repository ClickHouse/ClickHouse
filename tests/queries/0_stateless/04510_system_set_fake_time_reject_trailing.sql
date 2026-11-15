-- Trailing characters after the timestamp of `SYSTEM TEST VIEW ... SET FAKE TIME` must be rejected
-- instead of being silently dropped. The parser keeps the literal text, so the timestamp is parsed
-- and validated when the query is executed, which needs a refreshable materialized view.

CREATE MATERIALIZED VIEW test_view_04510 REFRESH EVERY 1 YEAR APPEND ENGINE = Memory AS SELECT 1 AS x;

SYSTEM TEST VIEW test_view_04510 SET FAKE TIME '2024-06-01 00:00:00';
SELECT 'valid accepted';

-- A valid timestamp followed by junk was silently accepted before; `assertEOF` now rejects it.
SYSTEM TEST VIEW test_view_04510 SET FAKE TIME '2024-06-01 00:00:00 junk'; -- { serverError CANNOT_PARSE_INPUT_ASSERTION_FAILED }

-- '2024 April 4' must not be silently reinterpreted as the timestamp 2024.
SYSTEM TEST VIEW test_view_04510 SET FAKE TIME '2024 April 4'; -- { serverError CANNOT_PARSE_DATETIME }

SYSTEM TEST VIEW test_view_04510 UNSET FAKE TIME;

DROP TABLE test_view_04510;
