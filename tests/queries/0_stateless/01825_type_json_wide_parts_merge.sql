-- Tags: no-fasttest

SET allow_experimental_object_type = 1;
SET output_format_json_named_tuples_as_objects = 1;

DROP TABLE IF EXISTS t_json_wide_parts;

CREATE TABLE t_json_wide_parts (data JSON)
ENGINE MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0;

SYSTEM STOP MERGES t_json_wide_parts;

INSERT INTO t_json_wide_parts VALUES ('{"k1": 1}');
INSERT INTO t_json_wide_parts VALUES ('{"k2": 2}');

SYSTEM START MERGES t_json_wide_parts;
OPTIMIZE TABLE t_json_wide_parts FINAL;

SELECT data FROM t_json_wide_parts ORDER BY data.k1 FORMAT JSONEachRow;

SELECT type FROM system.parts_columns
WHERE table = 't_json_wide_parts' AND database = currentDatabase() AND active;

DROP TABLE t_json_wide_parts;
