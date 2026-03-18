DROP TABLE IF EXISTS t_missed_subcolumns;

-- .null subcolumn

CREATE TABLE t_missed_subcolumns (x UInt32) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_missed_subcolumns SELECT * FROM numbers(10);

ALTER TABLE t_missed_subcolumns ADD COLUMN `y` Nullable(UInt32);

INSERT INTO t_missed_subcolumns SELECT number, if(number % 2, NULL, number) FROM numbers(10);

SELECT x FROM t_missed_subcolumns WHERE y IS NOT NULL SETTINGS optimize_functions_to_subcolumns = 1;
SELECT x FROM t_missed_subcolumns WHERE y IS NOT NULL SETTINGS optimize_functions_to_subcolumns = 0;

DROP TABLE IF EXISTS t_missed_subcolumns;

-- .null and .size0 subcolumn in array

CREATE TABLE t_missed_subcolumns (id UInt64, `n.a` Array(Nullable(String))) ENGINE = MergeTree ORDER BY id;

INSERT INTO t_missed_subcolumns VALUES (1, ['aaa', NULL]) (2, ['ccc']) (3, [NULL]);
ALTER TABLE t_missed_subcolumns ADD COLUMN `n.b` Array(Nullable(String));
INSERT INTO t_missed_subcolumns VALUES (4, [NULL, 'bbb'], ['ddd', NULL]), (5, [NULL], [NULL]);

SELECT id, n.a, n.b FROM t_missed_subcolumns ORDER BY id;
SELECT id, n.a.size0, n.b.size0 FROM t_missed_subcolumns ORDER BY id;
SELECT id, n.a.null, n.b.null FROM t_missed_subcolumns ORDER BY id;

DROP TABLE IF EXISTS t_missed_subcolumns;

-- subcolumns and custom defaults

CREATE TABLE t_missed_subcolumns (id UInt64) ENGINE = MergeTree ORDER BY id;
SYSTEM STOP MERGES t_missed_subcolumns;

INSERT INTO t_missed_subcolumns VALUES (1);

ALTER TABLE t_missed_subcolumns ADD COLUMN t Tuple(a String, b String) DEFAULT ('foo', 'bar');
INSERT INTO t_missed_subcolumns VALUES (2, ('aaa', 'bbb'));

ALTER TABLE t_missed_subcolumns ADD COLUMN arr Array(Nullable(UInt64)) DEFAULT [1, NULL, 3];
INSERT INTO t_missed_subcolumns VALUES (3, ('ccc', 'ddd'), [4, 5, 6]);

SELECT id, t, arr FROM t_missed_subcolumns ORDER BY id;
SELECT id, t.a, t.b, arr.size0, arr.null FROM t_missed_subcolumns ORDER BY id;

DROP TABLE t_missed_subcolumns;
