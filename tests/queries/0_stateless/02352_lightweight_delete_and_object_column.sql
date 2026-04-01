DROP TABLE IF EXISTS t_obj SYNC;

SET allow_experimental_object_type=1;

CREATE TABLE t_obj(id Int32, name Object('json')) ENGINE = MergeTree() ORDER BY id;

INSERT INTO t_obj select number, '{"a" : "' || toString(number) || '"}' FROM numbers(100);

DELETE FROM t_obj WHERE id = 10;

SELECT COUNT() FROM t_obj;

DROP TABLE t_obj SYNC;

