-- `RENAME COLUMN` must keep the meaning of a stored expression that uses column matcher
-- transformers: the replaced column name of `REPLACE` and the column references inside an `APPLY`
-- lambda are not plain identifier nodes of the expression tree, but they still name columns.

SET enable_named_columns_in_function_tuple = 0;

SELECT '-- REPLACE keeps matching the renamed column';

DROP TABLE IF EXISTS t_rename_replace;

CREATE TABLE t_rename_replace
(
    a UInt64,
    b UInt64,
    d String DEFAULT toJSONString(tuple(COLUMNS(a, b) REPLACE (a + 1 AS a)))
)
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_rename_replace (a, b) VALUES (1, 2);

ALTER TABLE t_rename_replace RENAME COLUMN a TO renamed;

SELECT default_expression FROM system.columns
WHERE database = currentDatabase() AND table = 't_rename_replace' AND name = 'd';

-- The REPLACE still applies, so the first element is `renamed + 1`, not `renamed`.
INSERT INTO t_rename_replace (renamed, b) VALUES (10, 20);
SELECT renamed, b, d FROM t_rename_replace ORDER BY renamed;

DROP TABLE t_rename_replace;

SELECT '-- an APPLY lambda body follows the rename';

DROP TABLE IF EXISTS t_rename_apply;

CREATE TABLE t_rename_apply
(
    a UInt64,
    b UInt64,
    d String DEFAULT toJSONString(tuple(COLUMNS('^b$') APPLY (x -> x + a)))
)
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_rename_apply (a, b) VALUES (1, 2);

ALTER TABLE t_rename_apply RENAME COLUMN a TO renamed;

SELECT default_expression FROM system.columns
WHERE database = currentDatabase() AND table = 't_rename_apply' AND name = 'd';

INSERT INTO t_rename_apply (renamed, b) VALUES (10, 20);
SELECT renamed, b, d FROM t_rename_apply ORDER BY renamed;

DROP TABLE t_rename_apply;

SELECT '-- a lambda argument shadowing the renamed column is left alone';

DROP TABLE IF EXISTS t_rename_shadowed;

CREATE TABLE t_rename_shadowed
(
    a UInt64,
    arr Array(UInt64),
    d String DEFAULT toJSONString(arrayMap(a -> a * 2, arr))
)
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_rename_shadowed (a, arr) VALUES (1, [2, 3]);

ALTER TABLE t_rename_shadowed RENAME COLUMN a TO renamed;

SELECT default_expression FROM system.columns
WHERE database = currentDatabase() AND table = 't_rename_shadowed' AND name = 'd';

INSERT INTO t_rename_shadowed (renamed, arr) VALUES (10, [4, 5]);
SELECT renamed, arr, d FROM t_rename_shadowed ORDER BY renamed;

DROP TABLE t_rename_shadowed;
