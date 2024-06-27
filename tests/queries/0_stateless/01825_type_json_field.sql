-- Tags: no-fasttest

SET allow_experimental_object_type = 1;

DROP TABLE IF EXISTS t_json_field;

CREATE TABLE t_json_field (id UInt32, data JSON)
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_json_field VALUES (1, (10, 'a')::Tuple(a UInt32, s String));

SELECT id, data.a, data.s FROM t_json_field ORDER BY id;
SELECT DISTINCT toTypeName(data) FROM t_json_field;

INSERT INTO t_json_field VALUES (2, ('sss', 300, 'b')::Tuple(a String, b UInt64, s String)), (3, (20, 'c')::Tuple(a UInt32, s String));

SELECT id, data.a, data.s, data.b FROM t_json_field ORDER BY id;
SELECT DISTINCT toTypeName(data) FROM t_json_field;

INSERT INTO t_json_field VALUES (4, map('a', 30, 'b', 400)), (5, map('s', 'qqq', 't', 'foo'));

SELECT id, data.a, data.s, data.b, data.t FROM t_json_field ORDER BY id;
SELECT DISTINCT toTypeName(data) FROM t_json_field;

INSERT INTO t_json_field VALUES (6, map(1, 2, 3, 4)); -- { clientError 53 }
INSERT INTO t_json_field VALUES (6, (1, 2, 3)); -- { clientError 53 }

DROP TABLE t_json_field;
