-- Regression test: a `DEFAULT` expression written inside a tuple element must not reference the
-- column it belongs to, neither directly nor through subcolumn syntax. `c Tuple(a UInt8, b UInt8
-- DEFAULT c.a)` used to be accepted, because only the tuple element names (`a`, `b`) were checked
-- for ambiguity, and the recursive-default detection in `ColumnsDescription` compares whole
-- identifiers, so it did not recognize `c.a` as a reference to the column `c` either. The table was
-- created with a self-referential default and failed only later, on an insert omitting the column.
-- See https://github.com/ClickHouse/ClickHouse/issues/2797.

DROP TABLE IF EXISTS t_tuple_default_self_reference;

SELECT '-- CREATE with a subcolumn self-reference';
CREATE TABLE t_tuple_default_self_reference (id UInt8, c Tuple(a UInt8, b UInt8 DEFAULT c.a)) ENGINE = MergeTree ORDER BY id; -- { serverError BAD_ARGUMENTS }

SELECT '-- CREATE with a direct self-reference';
CREATE TABLE t_tuple_default_self_reference (id UInt8, c Tuple(a UInt8, b String DEFAULT toString(c))) ENGINE = MergeTree ORDER BY id; -- { serverError BAD_ARGUMENTS }

SELECT '-- ALTER ADD COLUMN with a subcolumn self-reference';
CREATE TABLE t_tuple_default_self_reference (id UInt8) ENGINE = MergeTree ORDER BY id;
ALTER TABLE t_tuple_default_self_reference ADD COLUMN c Tuple(a UInt8, b UInt8 DEFAULT c.a); -- { serverError BAD_ARGUMENTS }

SELECT '-- ALTER MODIFY COLUMN with a subcolumn self-reference';
ALTER TABLE t_tuple_default_self_reference ADD COLUMN c Tuple(a UInt8, b UInt8);
ALTER TABLE t_tuple_default_self_reference MODIFY COLUMN c Tuple(a UInt8, b UInt8 DEFAULT c.a); -- { serverError BAD_ARGUMENTS }

SELECT '-- a reference to a subcolumn of another column is still allowed';
CREATE TABLE t_tuple_default_other_subcolumn
(
    other Tuple(a UInt8),
    c Tuple(a UInt8, b UInt8 DEFAULT other.a)
)
ENGINE = MergeTree ORDER BY tuple();
SELECT type, default_expression
FROM system.columns
WHERE database = currentDatabase() AND table = 't_tuple_default_other_subcolumn' AND name = 'c';
INSERT INTO t_tuple_default_other_subcolumn (other) VALUES ((3));
SELECT c FROM t_tuple_default_other_subcolumn;

DROP TABLE t_tuple_default_self_reference;
DROP TABLE t_tuple_default_other_subcolumn;
