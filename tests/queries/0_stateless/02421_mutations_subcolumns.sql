-- Tags: no-fasttest

DROP TABLE IF EXISTS t_mutations_subcolumns;

SET allow_experimental_object_type = 1;
SET allow_experimental_lightweight_delete = 1;

CREATE TABLE t_mutations_subcolumns (a UInt64, n Nullable(String), arr Array(UInt64), obj JSON)
ENGINE = MergeTree ORDER BY a;

INSERT INTO t_mutations_subcolumns VALUES (1, NULL, [1, 2],  '{"k1": {"k2": "foo"}, "k3": 4}');
INSERT INTO t_mutations_subcolumns VALUES (2, 'bbb', [],     '{"k1": {"k2": "foo"}, "k3": 4}');
INSERT INTO t_mutations_subcolumns VALUES (3, 'bbb', [1, 2], '{"k1": {"k2": "bar"}, "k3": 4}');
INSERT INTO t_mutations_subcolumns VALUES (4, 'bbb', [1, 2], '{"k1": {"k2": "foo"}, "k3": 5}');
INSERT INTO t_mutations_subcolumns VALUES (5, 'bbb', [1, 2], '{"k1": {"k2": "foo", "k4": "baz"}, "k3": 4}');
INSERT INTO t_mutations_subcolumns VALUES (6, 'aaa', [1, 2], '{"k1": {"k2": "foo"}, "k3": 4}');

OPTIMIZE TABLE t_mutations_subcolumns FINAL;

SELECT count(), min(a) FROM t_mutations_subcolumns;

SET mutations_sync = 2;

ALTER TABLE t_mutations_subcolumns DELETE WHERE n.null;
SELECT count(), min(a) FROM t_mutations_subcolumns;

DELETE FROM t_mutations_subcolumns WHERE arr.size0 = 0;
SELECT count(), min(a) FROM t_mutations_subcolumns;

ALTER TABLE t_mutations_subcolumns DELETE WHERE obj.k1.k2 = 'bar';
SELECT count(), min(a) FROM t_mutations_subcolumns;

DELETE FROM t_mutations_subcolumns WHERE obj.k3 = 5;
SELECT count(), min(a) FROM t_mutations_subcolumns;

ALTER TABLE t_mutations_subcolumns DELETE WHERE obj.k1 = ('foo', 'baz');
SELECT count(), min(a) FROM t_mutations_subcolumns;

ALTER TABLE t_mutations_subcolumns UPDATE n = 'ccc' WHERE obj.k1.k2 = 'foo';
SELECT n FROM t_mutations_subcolumns;

-- Update or modification of subcolumns currently is not supported.

ALTER TABLE t_mutations_subcolumns UPDATE `obj.k3` = 10 WHERE obj.k1.k2 = 'foo'; -- { serverError NO_SUCH_COLUMN_IN_TABLE }

ALTER TABLE t_mutations_subcolumns MODIFY COLUMN n.null String; -- { serverError NOT_FOUND_COLUMN_IN_BLOCK }
ALTER TABLE t_mutations_subcolumns DROP COLUMN arr.size0; -- { serverError NOT_FOUND_COLUMN_IN_BLOCK }

ALTER TABLE t_mutations_subcolumns MODIFY COLUMN `obj.k3` String; -- { serverError NOT_FOUND_COLUMN_IN_BLOCK }
ALTER TABLE t_mutations_subcolumns DROP COLUMN `obj.k3`; -- { serverError NOT_FOUND_COLUMN_IN_BLOCK }

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

TRUNCATE TABLE t_mutations_subcolumns;

INSERT INTO t_mutations_subcolumns VALUES (1, '{"k1": [{"k2": 1}, {"k2": 2}]}')
INSERT INTO t_mutations_subcolumns VALUES (2, '{"k1": [{"k3": 1}, {"k3": 2}, {"k3": 3}]}')
INSERT INTO t_mutations_subcolumns VALUES (3, '{"k1": [{"k2": 10, "k3": 10}]}');

SELECT count(), min(a) FROM t_mutations_subcolumns;

ALTER TABLE t_mutations_subcolumns DELETE WHERE obj.k1.k2 = [1, 2];
SELECT count(), min(a) FROM t_mutations_subcolumns;

ALTER TABLE t_mutations_subcolumns DELETE WHERE length(obj.k1.k3) = 3;
SELECT count(), min(a) FROM t_mutations_subcolumns;
