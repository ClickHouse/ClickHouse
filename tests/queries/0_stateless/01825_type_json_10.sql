-- Tags: no-fasttest

SET allow_experimental_object_type = 1;
SET output_format_json_named_tuples_as_objects = 1;

DROP TABLE IF EXISTS t_json_10;
CREATE TABLE t_json_10 (o Object('json')) ENGINE = Memory;

INSERT INTO t_json_10 FORMAT JSONAsObject {"a": {"b": 1, "c": [{"d": 10, "e": [31]}, {"d": 20, "e": [63, 127]}]}} {"a": {"b": 2, "c": []}}

INSERT INTO t_json_10 FORMAT JSONAsObject {"a": {"b": 3, "c": [{"f": 20, "e": [32]}, {"f": 30, "e": [64, 128]}]}} {"a": {"b": 4, "c": []}}

SELECT DISTINCT toTypeName(o) FROM t_json_10;
SELECT o FROM t_json_10 ORDER BY o.a.b FORMAT JSONEachRow;
SELECT o.a.b, o.a.c.d, o.a.c.e, o.a.c.f FROM t_json_10 ORDER BY o.a.b;

DROP TABLE t_json_10;
