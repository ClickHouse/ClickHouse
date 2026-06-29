-- DEFAULT expressions inside Tuple data types are pulled up to the column level.
-- https://github.com/ClickHouse/ClickHouse/issues/2797

DROP TABLE IF EXISTS t_default_in_tuple;
DROP TABLE IF EXISTS t_nested_tuple;
DROP TABLE IF EXISTS t_ref;
DROP TABLE IF EXISTS t_expr;

SELECT '-- basic';
CREATE TABLE t_default_in_tuple
(
    id UInt8,
    c Tuple(a UInt8, s String DEFAULT 'Hello')
)
ENGINE = MergeTree ORDER BY id;

-- The stored type is normalized: no DEFAULT inside the Tuple, a pulled-up column default instead.
SELECT name, type, default_kind, default_expression
FROM system.columns
WHERE database = currentDatabase() AND table = 't_default_in_tuple'
ORDER BY name;

INSERT INTO t_default_in_tuple (id) VALUES (1);
SELECT id, c FROM t_default_in_tuple;

SELECT '-- nested tuple';
CREATE TABLE t_nested_tuple
(
    id UInt8,
    c Tuple(a UInt8, t Tuple(x String DEFAULT 'q', y UInt8))
)
ENGINE = MergeTree ORDER BY id;
SELECT type, default_expression
FROM system.columns
WHERE database = currentDatabase() AND table = 't_nested_tuple' AND name = 'c';
INSERT INTO t_nested_tuple (id, c) VALUES (1, (5, ('z', 1)));
INSERT INTO t_nested_tuple (id) VALUES (2);
SELECT id, c FROM t_nested_tuple ORDER BY id;

SELECT '-- default referencing a column';
CREATE TABLE t_ref
(
    x UInt8,
    c Tuple(a UInt8 DEFAULT x, s String)
)
ENGINE = MergeTree ORDER BY x;
SELECT type, default_expression
FROM system.columns
WHERE database = currentDatabase() AND table = 't_ref' AND name = 'c';
INSERT INTO t_ref (x) VALUES (7);
SELECT c FROM t_ref;

SELECT '-- constant expression default';
CREATE TABLE t_expr
(
    id UInt8,
    c Tuple(a UInt8 DEFAULT 1 + 2, s String DEFAULT upper('hi'))
)
ENGINE = MergeTree ORDER BY id;
SELECT type, default_expression
FROM system.columns
WHERE database = currentDatabase() AND table = 't_expr' AND name = 'c';
INSERT INTO t_expr (id) VALUES (1);
SELECT c FROM t_expr;

SELECT '-- errors';
-- Referencing a sibling element (ambiguous with a column of the same name) is rejected.
CREATE TABLE t_amb (a UInt8, c Tuple(a UInt8, b UInt8 DEFAULT a)) ENGINE = Memory; -- { serverError BAD_ARGUMENTS }
-- Referencing another element of the same tuple is rejected.
CREATE TABLE t_sib (c Tuple(a UInt8, b UInt8 DEFAULT a)) ENGINE = Memory; -- { serverError BAD_ARGUMENTS }
-- A column-level default together with DEFAULTs inside the type is rejected.
CREATE TABLE t_conflict (c Tuple(a UInt8 DEFAULT 1) DEFAULT (2)) ENGINE = Memory; -- { serverError BAD_ARGUMENTS }
-- DEFAULT inside Nested is not supported.
CREATE TABLE t_nested (n Nested(x String, y UInt8 DEFAULT 5)) ENGINE = Memory; -- { serverError NOT_IMPLEMENTED }
-- DEFAULT inside Array is not supported.
CREATE TABLE t_array (c Array(Tuple(x UInt8 DEFAULT 5))) ENGINE = Memory; -- { serverError NOT_IMPLEMENTED }
-- Building a data type with a DEFAULT directly is rejected.
SELECT defaultValueOfTypeName('Tuple(a UInt8 DEFAULT 5)'); -- { serverError BAD_ARGUMENTS }
SELECT CAST((1, 2), 'Tuple(a UInt8, b UInt8 DEFAULT 5)'); -- { serverError BAD_ARGUMENTS }

-- The same normalization applies to ALTER, not only CREATE.
SELECT '-- alter add column';
DROP TABLE IF EXISTS t_alter_add;
CREATE TABLE t_alter_add (id UInt8) ENGINE = MergeTree ORDER BY id;
ALTER TABLE t_alter_add ADD COLUMN c Tuple(a UInt8, s String DEFAULT 'Hi');
SELECT type, default_kind, default_expression
FROM system.columns
WHERE database = currentDatabase() AND table = 't_alter_add' AND name = 'c';
INSERT INTO t_alter_add (id) VALUES (1);
SELECT id, c FROM t_alter_add;

SELECT '-- alter modify column';
DROP TABLE IF EXISTS t_alter_modify;
CREATE TABLE t_alter_modify (id UInt8, c Tuple(a UInt8, b UInt8)) ENGINE = MergeTree ORDER BY id;
ALTER TABLE t_alter_modify MODIFY COLUMN c Tuple(a UInt8, b UInt8 DEFAULT 42);
SELECT type, default_kind, default_expression
FROM system.columns
WHERE database = currentDatabase() AND table = 't_alter_modify' AND name = 'c';
INSERT INTO t_alter_modify (id) VALUES (1);
SELECT id, c FROM t_alter_modify;

SELECT '-- alter errors';
-- DEFAULT inside Nested is not supported on ALTER either.
ALTER TABLE t_alter_add ADD COLUMN n Nested(x String, y UInt8 DEFAULT 5); -- { serverError NOT_IMPLEMENTED }
-- DEFAULT inside Array is not supported on ALTER either.
ALTER TABLE t_alter_add MODIFY COLUMN c Array(Tuple(x UInt8 DEFAULT 5)); -- { serverError NOT_IMPLEMENTED }

DROP TABLE t_default_in_tuple;
DROP TABLE t_nested_tuple;
DROP TABLE t_ref;
DROP TABLE t_expr;
DROP TABLE t_alter_add;
DROP TABLE t_alter_modify;
