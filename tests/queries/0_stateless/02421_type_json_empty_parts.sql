-- Tags: no-fasttest

SET allow_experimental_object_type = 1;

DROP TABLE IF EXISTS t_json_empty_parts;

SELECT 'Collapsing';
CREATE TABLE t_json_empty_parts (id UInt64, s Int8, data JSON) ENGINE = CollapsingMergeTree(s) ORDER BY id;

INSERT INTO t_json_empty_parts VALUES (1, 1, '{"k1": "aaa"}') (1, -1, '{"k2": "bbb"}');

SELECT count() FROM t_json_empty_parts;
SELECT count() FROM system.parts WHERE table = 't_json_empty_parts' AND database = currentDatabase() AND active;
DESC TABLE t_json_empty_parts SETTINGS describe_extend_object_types = 1;

DROP TABLE t_json_empty_parts;

DROP TABLE IF EXISTS t_json_empty_parts;

SELECT 'DELETE all';
CREATE TABLE t_json_empty_parts (id UInt64, data JSON) ENGINE = MergeTree ORDER BY id;

INSERT INTO t_json_empty_parts VALUES (1, '{"k1": "aaa"}') (2, '{"k2": "bbb"}');

SELECT count() FROM t_json_empty_parts;
SELECT count() FROM system.parts WHERE table = 't_json_empty_parts' AND database = currentDatabase() AND active;
DESC TABLE t_json_empty_parts SETTINGS describe_extend_object_types = 1;

SET mutations_sync = 2;
ALTER TABLE t_json_empty_parts DELETE WHERE 1;

DETACH TABLE t_json_empty_parts;
ATTACH TABLE t_json_empty_parts;

SELECT count() FROM t_json_empty_parts;
SELECT count() FROM system.parts WHERE table = 't_json_empty_parts' AND database = currentDatabase() AND active;
DESC TABLE t_json_empty_parts SETTINGS describe_extend_object_types = 1;

DROP TABLE IF EXISTS t_json_empty_parts;

SELECT 'TTL';
CREATE TABLE t_json_empty_parts (id UInt64, d Date, data JSON) ENGINE = MergeTree ORDER BY id TTL d WHERE id % 2 = 1;

INSERT INTO t_json_empty_parts VALUES (1, '2000-01-01', '{"k1": "aaa"}') (2, '2000-01-01', '{"k2": "bbb"}');
OPTIMIZE TABLE t_json_empty_parts FINAL;

SELECT count() FROM t_json_empty_parts;
SELECT count() FROM system.parts WHERE table = 't_json_empty_parts' AND database = currentDatabase() AND active;
DESC TABLE t_json_empty_parts SETTINGS describe_extend_object_types = 1;

ALTER TABLE t_json_empty_parts MODIFY TTL d;
OPTIMIZE TABLE t_json_empty_parts FINAL;

DETACH TABLE t_json_empty_parts;
ATTACH TABLE t_json_empty_parts;

SELECT count() FROM t_json_empty_parts;
SELECT count() FROM system.parts WHERE table = 't_json_empty_parts' AND database = currentDatabase() AND active;
DESC TABLE t_json_empty_parts SETTINGS describe_extend_object_types = 1;

DROP TABLE IF EXISTS t_json_empty_parts;
