-- output_format_json_type_use_source writes the stored JSON text instead of constructing it.

DROP TABLE IF EXISTS t_json_source_output;
CREATE TABLE t_json_source_output (json JSON(with_source=1)) ENGINE = Memory;
INSERT INTO t_json_source_output VALUES ('{"a" :   42, "b" : "Hello"}');

SELECT json FROM t_json_source_output SETTINGS output_format_json_type_use_source = 1;
SELECT json FROM t_json_source_output SETTINGS output_format_json_type_use_source = 0;
SELECT toString(json) FROM t_json_source_output SETTINGS output_format_json_type_use_source = 1;

SELECT json FROM t_json_source_output FORMAT JSONEachRow SETTINGS output_format_json_type_use_source = 1;
SELECT json FROM t_json_source_output FORMAT CSV SETTINGS output_format_json_type_use_source = 1;
SELECT json FROM t_json_source_output FORMAT TSV SETTINGS output_format_json_type_use_source = 1;

-- Row formats write the stored text as is.
SELECT json FROM t_json_source_output FORMAT JSONEachRow SETTINGS output_format_json_type_use_source = 1, output_format_json_pretty_print = 1;

-- Pretty printing constructs the JSON from the object, because the stored text has its own formatting.
SELECT json FROM t_json_source_output FORMAT JSON SETTINGS output_format_json_type_use_source = 1, output_format_json_pretty_print = 1, output_format_write_statistics = 0;

-- The setting doesn't affect types without the source.
SELECT '{"a" :   42}'::JSON SETTINGS output_format_json_type_use_source = 1;

DROP TABLE t_json_source_output;
