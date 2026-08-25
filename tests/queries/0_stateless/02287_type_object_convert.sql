-- Tags: no-fasttest

SET allow_experimental_object_type = 1;

DROP TABLE IF EXISTS t_object_convert;

CREATE TABLE t_object_convert(id UInt64, data Object(Nullable('JSON'))) Engine=Memory;

INSERT INTO t_object_convert SELECT 1, CAST(CAST('{"x" : 1}', 'Object(\'json\')'), 'Object(Nullable(\'json\'))');
SELECT id, data, toTypeName(data) FROM t_object_convert ORDER BY id;
INSERT INTO t_object_convert SELECT 2, CAST(CAST('{"y" : 2}', 'Object(\'json\')'), 'Object(Nullable(\'json\'))');
SELECT id, data, toTypeName(data) FROM t_object_convert ORDER BY id;
INSERT INTO t_object_convert FORMAT JSONEachRow {"id": 3, "data": {"x": 1, "y" : 2}};

SELECT id, data, toTypeName(data) FROM t_object_convert ORDER BY id;
SELECT id, data.x, data.y FROM t_object_convert ORDER BY id;


CREATE TABLE t_object_convert2(id UInt64, data Object('JSON')) Engine=Memory;

INSERT INTO t_object_convert2 SELECT 1, CAST(CAST('{"x" : 1}', 'Object(\'json\')'), 'Object(Nullable(\'json\'))');
SELECT id, data, toTypeName(data) FROM t_object_convert2 ORDER BY id;
INSERT INTO t_object_convert2 SELECT 2, CAST(CAST('{"y" : 2}', 'Object(\'json\')'), 'Object(Nullable(\'json\'))');
SELECT id, data, toTypeName(data) FROM t_object_convert2 ORDER BY id;

DROP TABLE t_object_convert;
DROP TABLE t_object_convert2;

SELECT CAST(CAST('{"x" : 1}', 'Object(\'json\')'), 'Object(Nullable(\'json\'))');
SELECT CAST(CAST('{"x" : 1}', 'Object(Nullable(\'json\'))'), 'Object(\'json\')');
SELECT CAST('{"x" : [ 1 , [ 1 , 2] ]}', 'Object(\'json\')');
SELECT CAST('{"x" : [ {} , [ 1 , 2] ]}', 'Object(\'json\')');
SELECT CAST('{"x" : [ {} , [ 1 , [2]] ]}', 'Object(\'json\')');
SELECT CAST('{"x" : [ {} , [ {} , [2]] ]}', 'Object(\'json\')');
SELECT CAST(' {"a": { "a": [ [1],  null ] } }', 'Object(Nullable(\'json\'))');
