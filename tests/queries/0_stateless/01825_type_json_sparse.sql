-- Tags: no-fasttest

DROP TABLE IF EXISTS t_json_sparse;

SET allow_experimental_object_type = 1;
SET optimize_trivial_insert_select = 1;

CREATE TABLE t_json_sparse (data Object('json'))
ENGINE = MergeTree ORDER BY tuple()
SETTINGS ratio_of_defaults_for_sparse_serialization = 0.1,
min_bytes_for_wide_part = 0, index_granularity = 8192, index_granularity_bytes = '10Mi';

SYSTEM STOP MERGES t_json_sparse;

INSERT INTO t_json_sparse VALUES ('{"k1": 1, "k2": {"k3": 4}}');
INSERT INTO t_json_sparse SELECT '{"k1": 2}' FROM numbers(200000);

SELECT subcolumns.names, subcolumns.serializations, count() FROM system.parts_columns
ARRAY JOIN subcolumns
WHERE database = currentDatabase()
    AND table = 't_json_sparse' AND column = 'data' AND active
GROUP BY subcolumns.names, subcolumns.serializations
ORDER BY subcolumns.names;

SELECT '=============';

SYSTEM START MERGES t_json_sparse;
OPTIMIZE TABLE t_json_sparse FINAL;

SELECT subcolumns.names, subcolumns.serializations, count() FROM system.parts_columns
ARRAY JOIN subcolumns
WHERE database = currentDatabase()
    AND table = 't_json_sparse' AND column = 'data' AND active
GROUP BY subcolumns.names, subcolumns.serializations
ORDER BY subcolumns.names;

SELECT '=============';

DETACH TABLE t_json_sparse;
ATTACH TABLE t_json_sparse;

SELECT subcolumns.names, subcolumns.serializations, count() FROM system.parts_columns
ARRAY JOIN subcolumns
WHERE database = currentDatabase()
    AND table = 't_json_sparse' AND column = 'data' AND active
GROUP BY subcolumns.names, subcolumns.serializations
ORDER BY subcolumns.names;

INSERT INTO t_json_sparse SELECT '{"k1": 2}' FROM numbers(200000);

SELECT '=============';

OPTIMIZE TABLE t_json_sparse FINAL;

SELECT subcolumns.names, subcolumns.serializations, count() FROM system.parts_columns
ARRAY JOIN subcolumns
WHERE database = currentDatabase()
    AND table = 't_json_sparse' AND column = 'data' AND active
GROUP BY subcolumns.names, subcolumns.serializations
ORDER BY subcolumns.names;

SELECT data.k1, count(), sum(data.k2.k3) FROM t_json_sparse GROUP BY data.k1 ORDER BY data.k1;

-- DROP TABLE t_json_sparse;
