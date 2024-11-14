-- Tags: no-fasttest

DROP TABLE IF EXISTS t_subcolumns_sizes;

SET allow_experimental_object_type = 1;

CREATE TABLE t_subcolumns_sizes (id UInt64, arr Array(UInt64), n Nullable(String), d JSON)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO t_subcolumns_sizes FORMAT JSONEachRow {"id": 1, "arr": [1, 2, 3], "n": null, "d": {"k1": "v1", "k2": [{"k3": 1, "k4": "v2"}, {"k3": 3}]}}
INSERT INTO t_subcolumns_sizes FORMAT JSONEachRow {"id": 2, "arr": [0], "n": "foo", "d": {"k1": "v3", "k2": [{"k4": "v4"}, {"k3": "v5", "k5": 5}]}}

OPTIMIZE TABLE t_subcolumns_sizes FINAL;

SELECT
    column,
    subcolumns.names AS sname,
    subcolumns.types AS stype,
    subcolumns.bytes_on_disk > 0
FROM system.parts_columns ARRAY JOIN subcolumns
WHERE database = currentDatabase() AND table = 't_subcolumns_sizes' AND active
ORDER BY column, sname, stype;

SELECT
    any(column_bytes_on_disk) = sum(subcolumns.bytes_on_disk),
    any(column_data_compressed_bytes) = sum(subcolumns.data_compressed_bytes),
    any(column_data_uncompressed_bytes) = sum(subcolumns.data_uncompressed_bytes),
    any(column_marks_bytes) = sum(subcolumns.marks_bytes)
FROM system.parts_columns ARRAY JOIN subcolumns
WHERE database = currentDatabase() AND table = 't_subcolumns_sizes'
AND active AND column = 'd';

DROP TABLE IF EXISTS t_subcolumns_sizes;
