DROP TABLE IF EXISTS t_missed_subcolumns;

SET allow_experimental_object_type = 1;

CREATE TABLE t_missed_subcolumns (id UInt64, n String, obj Object(Nullable('json')))
ENGINE = MergeTree ORDER BY id;

INSERT INTO t_missed_subcolumns VALUES (1, 'aaa', '{"k1": {"k2": "foo"}, "k3": 5}');
INSERT INTO t_missed_subcolumns VALUES (2, 'bbb', '{"k1": {"k2": "fee"}, "k3": 4}');
INSERT INTO t_missed_subcolumns VALUES (3, 'ccc', '{"k1": {"k2": "foo", "k4": "baz"}, "k3": 4}');
INSERT INTO t_missed_subcolumns VALUES (4, 'ddd', '{"k1": {"k2": "foo"}, "k3": 4}');

OPTIMIZE TABLE t_missed_subcolumns FINAL;

SELECT count(), min(id) FROM t_missed_subcolumns;

SELECT * FROM t_missed_subcolumns WHERE obj.k4 = 5 ORDER BY id FORMAT JSONEachRow;

SELECT * FROM t_missed_subcolumns WHERE obj.k1.k3 = 'fee' ORDER BY id FORMAT JSONEachRow;

SELECT id, n, obj.k4 FROM t_missed_subcolumns ORDER BY id FORMAT JSONEachRow;

ALTER TABLE t_missed_subcolumns DELETE WHERE obj.k4 = 5;
SELECT count(), min(id) FROM t_missed_subcolumns;

DELETE FROM t_missed_subcolumns WHERE obj.k1.k3 = 'fee';
SELECT count(), min(id) FROM t_missed_subcolumns;

DROP TABLE IF EXISTS t_missed_subcolumns;
