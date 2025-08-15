-- Tags: no-parallel
-- (databases can be removed in background, so this test should not be run in parallel)

DROP TABLE IF EXISTS t;
CREATE TABLE t (b UInt8) ENGINE = Memory;
SELECT a FROM merge(REGEXP('.'), '^t$'); -- { serverError UNKNOWN_IDENTIFIER }
SELECT a FROM merge(REGEXP('\0'), '^t$'); -- { serverError UNKNOWN_IDENTIFIER }
SELECT a FROM merge(REGEXP('\0a'), '^t$'); -- { serverError UNKNOWN_IDENTIFIER }
SELECT a FROM merge(REGEXP('\0a'), '^$'); -- { serverError BAD_ARGUMENTS }
DROP TABLE t;
