-- The query-construction settings (`select` / `filter` / `order` / `sort` / `limit` / `offset` /
-- `page`) shape a query's result. When carried by the session context via a `SET`, they behave like a
-- session setting on any other query: they apply to a single outermost result-producing `SELECT`, but
-- do not propagate into an `INSERT` / `CREATE`'s source `SELECT` (nor into subqueries). So a session
-- `SET limit = ...` followed by `INSERT ... SELECT` / `CREATE ... AS SELECT` processes all rows.
-- The `SET` is cleared right after each statement so the verification queries are not themselves shaped.

DROP TABLE IF EXISTS t_src;
DROP TABLE IF EXISTS t_dst;
CREATE TABLE t_src (x UInt64) ENGINE = Memory AS SELECT number FROM numbers(10);
CREATE TABLE t_dst (x UInt64) ENGINE = Memory;

SELECT '-- a session SET limit shapes a plain SELECT (2 rows returned)';
SET limit = 2;
SELECT x FROM t_src ORDER BY x;
SET limit = 0;

SELECT '-- but a session SET limit does not propagate into INSERT ... SELECT (all 10 rows)';
SET limit = 2;
INSERT INTO t_dst SELECT x FROM t_src;
SET limit = 0;
SELECT count() FROM t_dst;

SELECT '-- a session SET filter does not propagate into INSERT ... SELECT (all 10 rows)';
TRUNCATE TABLE t_dst;
SET filter = 'x > 100';
INSERT INTO t_dst SELECT x FROM t_src;
SET filter = '';
SELECT count() FROM t_dst;

SELECT '-- a session SET sort does not block CREATE ... AS SELECT (table created, all 10 rows)';
DROP TABLE IF EXISTS t_cas;
SET sort = 'x';
CREATE TABLE t_cas ENGINE = Memory AS SELECT x FROM t_src;
SET sort = '';
SELECT count() FROM t_cas;

SELECT '-- a repair SET is not rejected by construction-setting validation (page without limit)';
-- Storing `page` without `limit` is invalid for a result query, but a `SET` must not be validated as
-- if it were one: otherwise the repair `SET page = 0` is rejected (reading the still-stored page != 0,
-- limit = 0) before InterpreterSetQuery can clear it.
SET page = 2;
SET page = 0;
SELECT 'page repaired';

SELECT '-- a repair SET clears a stored order/sort conflict without being rejected first';
SET sort = 'x';
SET order = 'x';
SET sort = '';
SET order = '';
SELECT 'order/sort repaired';

DROP TABLE t_cas;
DROP TABLE t_dst;
DROP TABLE t_src;
