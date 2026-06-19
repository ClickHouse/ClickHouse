-- The query-construction settings (`select` / `filter` / `order` / `sort` / `limit` / `offset` /
-- `page`) shape the result a query returns to the client, so they are rejected on write-producing
-- statements (`INSERT ... SELECT`, `CREATE ... AS SELECT`) rather than silently ignored. This must
-- hold not only when the setting appears in the query's own `SETTINGS` clause (see `04345`), but also
-- when it is carried by the session context via a `SET` — otherwise the write would silently process
-- all rows. The `SET` is cleared right after each rejected statement so the verification queries below
-- (and the test harness) are not themselves wrapped by the still-active setting.

DROP TABLE IF EXISTS t_src;
DROP TABLE IF EXISTS t_dst;
CREATE TABLE t_src (x UInt64) ENGINE = Memory AS SELECT number FROM numbers(10);
CREATE TABLE t_dst (x UInt64) ENGINE = Memory;

SELECT '-- session SET limit then INSERT ... SELECT is rejected';
SET limit = 2;
INSERT INTO t_dst SELECT x FROM t_src; -- { serverError BAD_ARGUMENTS }
SET limit = 0;

SELECT '-- session SET offset then INSERT ... SELECT is rejected';
SET offset = 3;
INSERT INTO t_dst SELECT x FROM t_src; -- { serverError BAD_ARGUMENTS }
SET offset = 0;

SELECT '-- session SET filter then INSERT ... SELECT is rejected';
SET filter = 'x > 0';
INSERT INTO t_dst SELECT x FROM t_src; -- { serverError BAD_ARGUMENTS }
SET filter = '';

SELECT '-- session SET sort then CREATE ... AS SELECT is rejected (table not created)';
DROP TABLE IF EXISTS t_cas;
SET sort = 'x';
CREATE TABLE t_cas ENGINE = Memory AS SELECT x FROM t_src; -- { serverError BAD_ARGUMENTS }
SET sort = '';
EXISTS t_cas;

SELECT '-- nothing was inserted by any rejected statement';
SELECT count() FROM t_dst;

SELECT '-- a non-construction session setting does not block INSERT ... SELECT';
SET max_block_size = 100;
INSERT INTO t_dst SELECT x FROM t_src;
SET max_block_size = DEFAULT;
SELECT count() FROM t_dst;

SELECT '-- with the construction settings cleared, CREATE ... AS SELECT works';
CREATE TABLE t_cas ENGINE = Memory AS SELECT x FROM t_src;
SELECT count() FROM t_cas;

DROP TABLE t_cas;
DROP TABLE t_dst;
DROP TABLE t_src;
