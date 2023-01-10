-- Tags: no-fasttest

DROP TABLE IF EXISTS t_json_mutations;

SET allow_experimental_object_type = 1;
SET output_format_json_named_tuples_as_objects = 1;
SET mutations_sync = 2;

CREATE TABLE t_json_mutations(id UInt32, s String, obj JSON) ENGINE = MergeTree ORDER BY id;

INSERT INTO t_json_mutations VALUES (1, 'q', '{"k1": 1, "k2": 2, "k3": [{"k4": "aaa"}, {"k4": "bbb"}]}');
INSERT INTO t_json_mutations VALUES (2, 'w', '{"k1": 3, "k2": 4, "k3": [{"k4": "ccc"}]}');
INSERT INTO t_json_mutations VALUES (3, 'e', '{"k1": 5, "k2": 6}');

SELECT * FROM t_json_mutations ORDER BY id;
ALTER TABLE t_json_mutations DELETE WHERE id = 2;
SELECT * FROM t_json_mutations ORDER BY id;
ALTER TABLE t_json_mutations DROP COLUMN s, DROP COLUMN obj, ADD COLUMN t String DEFAULT 'foo';
SELECT * FROM t_json_mutations ORDER BY id;

DROP TABLE t_json_mutations;
