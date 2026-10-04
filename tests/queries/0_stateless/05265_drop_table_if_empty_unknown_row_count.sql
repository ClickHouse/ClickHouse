-- Tags: no-parallel-replicas
-- `DROP TABLE ... IF EMPTY` must not take an unknown row count for zero. A `File` table does not
-- derive a row count from metadata, so the drop is refused whether it holds rows or not: the table
-- is not read to find out. A plain view stores no rows, so it is dropped.

DROP TABLE IF EXISTS t_if_empty_full;
DROP TABLE IF EXISTS t_if_empty_no_rows;
DROP TABLE IF EXISTS t_if_empty_view;

CREATE TABLE t_if_empty_full (x UInt64) ENGINE = File(TSV);
INSERT INTO t_if_empty_full VALUES (1), (2), (3);
CREATE TABLE t_if_empty_no_rows (x UInt64) ENGINE = File(TSV);
INSERT INTO t_if_empty_no_rows SELECT number FROM numbers(0);
CREATE VIEW t_if_empty_view AS SELECT number AS x FROM numbers(3);

SELECT name, total_rows FROM system.tables WHERE database = currentDatabase() AND name LIKE 't_if_empty%' ORDER BY name;

DROP TABLE IF EMPTY t_if_empty_full SETTINGS ignore_drop_queries_probability = 0; -- { serverError TABLE_NOT_EMPTY }
DROP TABLE IF EMPTY t_if_empty_no_rows SETTINGS ignore_drop_queries_probability = 0; -- { serverError TABLE_NOT_EMPTY }
DROP TABLE IF EMPTY t_if_empty_view SETTINGS ignore_drop_queries_probability = 0;

SELECT name FROM system.tables WHERE database = currentDatabase() AND name LIKE 't_if_empty%' ORDER BY name;
SELECT count() FROM t_if_empty_full;

DROP TABLE t_if_empty_full;
DROP TABLE t_if_empty_no_rows;
