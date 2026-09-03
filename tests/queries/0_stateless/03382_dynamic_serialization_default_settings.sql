SET enable_json_type = 1;
DROP TABLE IF EXISTS t0;
CREATE TABLE t0 (c0 JSON(max_dynamic_types=0)) ENGINE = Memory;
INSERT INTO TABLE t0 (c0) SETTINGS input_format_binary_read_json_as_string = 1, output_format_native_write_json_as_string = 1 VALUES ('{"a":[{},1]}');
SELECT * FROM t0;
DROP TABLE t0;

