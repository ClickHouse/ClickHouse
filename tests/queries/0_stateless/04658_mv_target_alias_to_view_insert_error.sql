-- A materialized view must not push data into a normal view, even when the view hides behind a
-- table with the `Alias` engine: the view would forward the push back into its underlying table,
-- which is the source of the materialized view, and the insert would recurse forever.

SET allow_experimental_alias_table_engine = 1;

DROP TABLE IF EXISTS t_04658;
DROP VIEW IF EXISTS v_04658;
DROP TABLE IF EXISTS a_04658;
DROP TABLE IF EXISTS mv_04658;

CREATE TABLE t_04658 (x Int64) ENGINE = MergeTree ORDER BY x;
CREATE VIEW v_04658 AS SELECT x FROM t_04658;
CREATE TABLE a_04658 ENGINE = Alias('v_04658');
CREATE MATERIALIZED VIEW mv_04658 TO a_04658 AS SELECT x FROM t_04658;

-- The push from mv_04658 resolves through the alias to the view and is rejected.
INSERT INTO t_04658 VALUES (1); -- { serverError NOT_IMPLEMENTED }

-- A direct insert into the alias is forwarded through the view into t_04658,
-- whose own push to mv_04658 hits the same rejection.
INSERT INTO a_04658 VALUES (2); -- { serverError NOT_IMPLEMENTED }

-- The failed inserts must not leave any rows behind.
SELECT count() FROM t_04658;

DROP TABLE mv_04658;
DROP TABLE a_04658;
DROP VIEW v_04658;
DROP TABLE t_04658;
