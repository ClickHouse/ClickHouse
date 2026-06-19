-- The query-construction settings (`select` / `filter` / `order` / `sort` / `limit` / `offset` /
-- `page`) shape the result a query returns to the client. They are not supported on write-producing
-- statements (`INSERT ... SELECT`, `CREATE ... AS SELECT`), which return no such result, and are
-- rejected with a clear error rather than silently ignored. Non-construction settings still work.

DROP TABLE IF EXISTS t_src;
DROP TABLE IF EXISTS t_dst;
CREATE TABLE t_src (x UInt64) ENGINE = Memory AS SELECT number FROM numbers(10);
CREATE TABLE t_dst (x UInt64) ENGINE = Memory;

SELECT '-- INSERT ... SELECT with a construction setting in the INSERT SETTINGS clause is rejected';
INSERT INTO t_dst SETTINGS filter = 'x > 0' SELECT x FROM t_src; -- { serverError BAD_ARGUMENTS }
INSERT INTO t_dst SETTINGS limit = 2 SELECT x FROM t_src; -- { serverError BAD_ARGUMENTS }
INSERT INTO t_dst SETTINGS offset = 1 SELECT x FROM t_src; -- { serverError BAD_ARGUMENTS }
INSERT INTO t_dst SETTINGS sort = 'x' SELECT x FROM t_src; -- { serverError BAD_ARGUMENTS }

SELECT '-- INSERT ... SELECT with a construction setting on the source SELECT is rejected';
INSERT INTO t_dst SELECT x FROM t_src SETTINGS limit = 2; -- { serverError BAD_ARGUMENTS }

SELECT '-- a construction setting in a nested subquery of the source is rejected too (would be a silent no-op)';
INSERT INTO t_dst SELECT x FROM (SELECT x FROM t_src SETTINGS limit = 2); -- { serverError BAD_ARGUMENTS }

SELECT '-- nothing was inserted by any rejected statement';
SELECT count() FROM t_dst;

SELECT '-- a non-construction setting on INSERT ... SELECT still works (all 10 rows inserted)';
INSERT INTO t_dst SETTINGS max_block_size = 100 SELECT x FROM t_src;
SELECT count() FROM t_dst;

SELECT '-- an explicit LIMIT clause in the source SELECT is not a construction setting and still works';
TRUNCATE TABLE t_dst;
INSERT INTO t_dst SELECT x FROM t_src ORDER BY x LIMIT 3;
SELECT count() FROM t_dst;

SELECT '-- CREATE ... AS SELECT with a construction setting is rejected (table not created)';
DROP TABLE IF EXISTS t_cas;
CREATE TABLE t_cas ENGINE = Memory AS SELECT x FROM t_src SETTINGS filter = 'x > 0'; -- { serverError BAD_ARGUMENTS }
EXISTS t_cas;

SELECT '-- CREATE ... AS SELECT without construction settings still works';
CREATE TABLE t_cas ENGINE = Memory AS SELECT x FROM t_src;
SELECT count() FROM t_cas;

DROP TABLE t_cas;
DROP TABLE t_dst;
DROP TABLE t_src;
