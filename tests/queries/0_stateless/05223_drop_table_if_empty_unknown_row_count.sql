-- Tags: no-parallel-replicas
-- `DROP TABLE ... IF EMPTY` must not take an unknown row count for zero. Neither a `File` table
-- nor a view derives a row count from metadata, so the table is read instead: the drop is refused
-- as soon as a row comes out, and goes ahead when the read finishes without one.

DROP TABLE IF EXISTS t_if_empty_full;
DROP TABLE IF EXISTS t_if_empty_empty;

CREATE TABLE t_if_empty_full (x UInt64) ENGINE = File(TSV);
INSERT INTO t_if_empty_full VALUES (1), (2), (3);
CREATE VIEW t_if_empty_empty AS SELECT number AS x FROM numbers(0);

SELECT name, total_rows FROM system.tables WHERE database = currentDatabase() AND name LIKE 't_if_empty%' ORDER BY name;

DROP TABLE IF EMPTY t_if_empty_full SETTINGS ignore_drop_queries_probability = 0; -- { serverError TABLE_NOT_EMPTY }
DROP TABLE IF EMPTY t_if_empty_empty SETTINGS ignore_drop_queries_probability = 0;

SELECT name FROM system.tables WHERE database = currentDatabase() AND name LIKE 't_if_empty%' ORDER BY name;
SELECT count() FROM t_if_empty_full;

DROP TABLE t_if_empty_full;
