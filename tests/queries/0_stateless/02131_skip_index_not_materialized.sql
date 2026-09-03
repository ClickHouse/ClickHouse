DROP TABLE IF EXISTS t_index_non_materialized;

CREATE TABLE t_index_non_materialized (a UInt32) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_index_non_materialized VALUES (1);

ALTER TABLE t_index_non_materialized ADD INDEX ind_set (a) TYPE set(1) GRANULARITY 1;
ALTER TABLE t_index_non_materialized ADD INDEX ind_minmax (a) TYPE minmax() GRANULARITY 1;

SELECT count() FROM t_index_non_materialized WHERE a = 1;

DROP TABLE t_index_non_materialized;
