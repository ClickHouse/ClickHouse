-- `DROP TABLE ... IF EMPTY` and `TRUNCATE TABLE ... IF EMPTY` on a temporary table check the
-- emptiness like on an ordinary table, and a materialized view with a `TO` table, which keeps no
-- rows of its own, is dropped regardless of the rows of its target table.

CREATE TEMPORARY TABLE tmp_if_empty (x UInt64);
INSERT INTO tmp_if_empty VALUES (1), (2), (3);
DROP TEMPORARY TABLE IF EMPTY tmp_if_empty; -- { serverError TABLE_NOT_EMPTY }
DROP TABLE IF EMPTY tmp_if_empty; -- { serverError TABLE_NOT_EMPTY }
TRUNCATE TEMPORARY TABLE IF EMPTY tmp_if_empty; -- { serverError TABLE_NOT_EMPTY }
SELECT count() FROM tmp_if_empty;
TRUNCATE TEMPORARY TABLE tmp_if_empty;
DROP TEMPORARY TABLE IF EMPTY tmp_if_empty;
SELECT count() FROM system.tables WHERE is_temporary AND name = 'tmp_if_empty';

DROP TABLE IF EXISTS t_if_empty_src;
DROP TABLE IF EXISTS t_if_empty_dst;
DROP TABLE IF EXISTS t_if_empty_mv;
CREATE TABLE t_if_empty_src (x UInt64) ENGINE = MergeTree ORDER BY x;
CREATE TABLE t_if_empty_dst (x UInt64) ENGINE = MergeTree ORDER BY x;
CREATE MATERIALIZED VIEW t_if_empty_mv TO t_if_empty_dst AS SELECT x FROM t_if_empty_src;
INSERT INTO t_if_empty_src VALUES (1), (2), (3);
DROP TABLE IF EMPTY t_if_empty_mv SETTINGS ignore_drop_queries_probability = 0;
SELECT name FROM system.tables WHERE database = currentDatabase() AND name LIKE 't_if_empty%' ORDER BY name;
SELECT count() FROM t_if_empty_dst;
DROP TABLE t_if_empty_src;
DROP TABLE t_if_empty_dst;
