-- Tags: no-parallel
-- (databases can be removed in background, so this test should not be run in parallel)

DROP TABLE IF EXISTS t;
CREATE TABLE t (b UInt8) ENGINE = Memory;
SELECT a FROM merge(REGEXP('.'), '^t$'); -- { serverError UNKNOWN_IDENTIFIER }
-- A NUL (`\0`) byte is an ordinary literal in the pattern (re2 is binary-safe), so these database
-- regexps match no database (none has a NUL in its name) instead of being silently truncated to an
-- empty, match-everything pattern. Hence no table is found.
SELECT a FROM merge(REGEXP('\0'), '^t$'); -- { serverError CANNOT_EXTRACT_TABLE_STRUCTURE }
SELECT a FROM merge(REGEXP('\0a'), '^t$'); -- { serverError CANNOT_EXTRACT_TABLE_STRUCTURE }
SELECT a FROM merge(REGEXP('\0a'), '^$'); -- { serverError CANNOT_EXTRACT_TABLE_STRUCTURE }
DROP TABLE t;
