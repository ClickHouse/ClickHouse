-- Tags: no-parallel
-- (databases can be removed in background, so this test should not be run in parallel)

DROP TABLE IF EXISTS t;
CREATE TABLE t (b UInt8) ENGINE = Memory;
SELECT a FROM merge(REGEXP('.'), '^t$'); -- { serverError 47 }
SELECT a FROM merge(REGEXP('\0'), '^t$'); -- { serverError 47 }
SELECT a FROM merge(REGEXP('\0a'), '^t$'); -- { serverError 47 }
SELECT a FROM merge(REGEXP('\0a'), '^$'); -- { serverError 36 }
DROP TABLE t;
