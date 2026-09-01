DROP TABLE IF EXISTS t_materialize_column;

CREATE TABLE t_materialize_column (i Int32)
ENGINE = MergeTree ORDER BY i PARTITION BY i
SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO t_materialize_column VALUES (1);

ALTER TABLE t_materialize_column ADD COLUMN s LowCardinality(String) DEFAULT toString(i);
ALTER TABLE t_materialize_column MATERIALIZE COLUMN s SETTINGS mutations_sync = 2;

SELECT name, column, type FROM system.parts_columns
WHERE table = 't_materialize_column' AND database = currentDatabase() AND active
ORDER BY name, column;

SELECT '===========';

INSERT INTO t_materialize_column (i) VALUES (2);

SELECT name, column, type FROM system.parts_columns
WHERE table = 't_materialize_column' AND database = currentDatabase() AND active
ORDER BY name, column;

SELECT '===========';

ALTER TABLE t_materialize_column ADD INDEX s_bf (s) TYPE bloom_filter(0.01) GRANULARITY 1;
ALTER TABLE t_materialize_column MATERIALIZE INDEX s_bf SETTINGS mutations_sync = 2;

SELECT name, column, type FROM system.parts_columns
WHERE table = 't_materialize_column' AND database = currentDatabase() AND active
ORDER BY name, column;

SELECT * FROM t_materialize_column ORDER BY i;

DROP TABLE t_materialize_column;
