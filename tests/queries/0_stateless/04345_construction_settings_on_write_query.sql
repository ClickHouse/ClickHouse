-- The query-construction settings (`select` / `filter` / `order` / `sort` / `limit` / `offset` /
-- `page`) shape a query's result. They follow the same scope rules as any other setting: a setting in
-- a source `SELECT`'s own `SETTINGS` clause shapes that `SELECT` (so it is effective for the source of
-- `INSERT ... SELECT` / `CREATE ... AS SELECT`), while a setting on the `INSERT` / `CREATE` statement
-- itself does not propagate into the `SELECT` and therefore has no effect on the rows it produces.

DROP TABLE IF EXISTS t_src;
DROP TABLE IF EXISTS t_dst;
CREATE TABLE t_src (x UInt64) ENGINE = Memory AS SELECT number FROM numbers(10);
CREATE TABLE t_dst (x UInt64) ENGINE = Memory;

SELECT '-- a `limit` on the source SELECT is effective: only 2 rows are inserted';
INSERT INTO t_dst SELECT x FROM t_src SETTINGS limit = 2;
SELECT count() FROM t_dst;

SELECT '-- a `filter` on the source SELECT is effective: only rows with x >= 7 are inserted';
TRUNCATE TABLE t_dst;
INSERT INTO t_dst SELECT x FROM t_src SETTINGS filter = 'x >= 7';
SELECT count() FROM t_dst;
SELECT * FROM t_dst ORDER BY x;

SELECT '-- a construction setting in a nested subquery of the source is effective for that subquery';
TRUNCATE TABLE t_dst;
INSERT INTO t_dst SELECT x FROM (SELECT x FROM t_src SETTINGS limit = 2);
SELECT count() FROM t_dst;

SELECT '-- a construction setting on the INSERT statement itself does not propagate to the SELECT (all 10 rows)';
TRUNCATE TABLE t_dst;
INSERT INTO t_dst SETTINGS limit = 2 SELECT x FROM t_src;
SELECT count() FROM t_dst;

SELECT '-- a non-construction setting on INSERT ... SELECT still works (all 10 rows)';
TRUNCATE TABLE t_dst;
INSERT INTO t_dst SETTINGS max_block_size = 100 SELECT x FROM t_src;
SELECT count() FROM t_dst;

SELECT '-- an explicit LIMIT clause in the source SELECT still works';
TRUNCATE TABLE t_dst;
INSERT INTO t_dst SELECT x FROM t_src ORDER BY x LIMIT 3;
SELECT count() FROM t_dst;

SELECT '-- CREATE ... AS SELECT with a construction setting on the SELECT is effective (9 rows: x > 0)';
DROP TABLE IF EXISTS t_cas;
CREATE TABLE t_cas ENGINE = Memory AS SELECT x FROM t_src SETTINGS filter = 'x > 0';
SELECT count() FROM t_cas;

SELECT '-- CREATE ... AS SELECT without construction settings still works (all 10 rows)';
DROP TABLE t_cas;
CREATE TABLE t_cas ENGINE = Memory AS SELECT x FROM t_src;
SELECT count() FROM t_cas;

DROP TABLE t_cas;
DROP TABLE t_dst;
DROP TABLE t_src;
