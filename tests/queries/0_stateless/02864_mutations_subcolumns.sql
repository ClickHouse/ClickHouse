-- Tags: no-replicated-database
-- It won't work in case there are misssing subcolumns in different shards

DROP TABLE IF EXISTS t_mutations_subcolumns;

SET allow_experimental_object_type = 1;

CREATE TABLE t_mutations_subcolumns (id UInt64, n String, obj JSON)
ENGINE = MergeTree ORDER BY id;

INSERT INTO t_mutations_subcolumns VALUES (1, 'aaa', '{"k1": {"k2": "foo"}, "k3": 5}');
INSERT INTO t_mutations_subcolumns VALUES (2, 'bbb', '{"k1": {"k2": "fee"}, "k3": 4}');
INSERT INTO t_mutations_subcolumns VALUES (3, 'ccc', '{"k1": {"k2": "foo", "k4": "baz"}, "k3": 4}');
INSERT INTO t_mutations_subcolumns VALUES (4, 'ddd', '{"k1": {"k2": "foo"}, "k3": 4}');
INSERT INTO t_mutations_subcolumns VALUES (5, 'eee', '{"k1": {"k2": "foo"}, "k3": 4}');
INSERT INTO t_mutations_subcolumns VALUES (6, 'fff', '{"k1": {"k2": "foo"}, "k3": 4}');

OPTIMIZE TABLE t_mutations_subcolumns FINAL;

SELECT count(), min(id) FROM t_mutations_subcolumns;

SET mutations_sync = 2;

ALTER TABLE t_mutations_subcolumns DELETE WHERE obj.k3 = 5;
SELECT count(), min(id) FROM t_mutations_subcolumns;  

DELETE FROM t_mutations_subcolumns WHERE obj.k1.k2 = 'fee';
SELECT count(), min(id) FROM t_mutations_subcolumns;

ALTER TABLE t_mutations_subcolumns DELETE WHERE obj.k1 = ('foo', 'baz');
SELECT count(), min(id) FROM t_mutations_subcolumns;

ALTER TABLE t_mutations_subcolumns UPDATE n = 'ttt' WHERE obj.k1.k2 = 'foo';
SELECT id, n FROM t_mutations_subcolumns;

DROP TABLE IF EXISTS t_mutations_subcolumns;

CREATE TABLE t_mutations_subcolumns (a UInt64, obj JSON)
ENGINE = MergeTree ORDER BY a PARTITION BY a;

INSERT INTO t_mutations_subcolumns VALUES (1, '{"k1": 1}');
INSERT INTO t_mutations_subcolumns VALUES (2, '{"k2": 1}');
INSERT INTO t_mutations_subcolumns VALUES (3, '{"k3": 1}');

ALTER TABLE t_mutations_subcolumns DELETE WHERE obj.k2 = 1;
SELECT * FROM t_mutations_subcolumns ORDER BY a FORMAT JSONEachRow;

ALTER TABLE t_mutations_subcolumns DELETE WHERE obj.k1 = 0;
SELECT * FROM t_mutations_subcolumns ORDER BY a FORMAT JSONEachRow;

DROP TABLE t_mutations_subcolumns;
