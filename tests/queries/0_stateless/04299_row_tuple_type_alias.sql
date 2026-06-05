-- ROW(...) accepted as a SQL-standard alias for Tuple(...).
-- https://github.com/ClickHouse/ClickHouse/issues/105954

-- The alias resolves to exactly the Tuple type, case-insensitively.
SELECT toTypeName(CAST((1, 'a') AS ROW(x Int32, y String))) = toTypeName(CAST((1, 'a') AS Tuple(x Int32, y String)));
SELECT toTypeName(CAST((1, 'a') AS row(x Int32, y String))) = toTypeName(CAST((1, 'a') AS Tuple(x Int32, y String)));

-- Named-field access works through the alias.
SELECT (CAST((1, 'a') AS ROW(x Int32, y String))).x;
SELECT (CAST((1, 'a') AS ROW(x Int32, y String))).y;

-- Standard-SQL inner type spellings from the issue (INTEGER, VARCHAR(N)) resolve too.
SELECT toTypeName(CAST((1, 'a') AS ROW(x INTEGER, y VARCHAR(10)))) = toTypeName(CAST((1, 'a') AS Tuple(x Int32, y String)));

-- ROW(...) in column DDL.
DROP TABLE IF EXISTS t_row_alias;
CREATE TABLE t_row_alias (r ROW(id Int32, name String)) ENGINE = Memory;
INSERT INTO t_row_alias VALUES ((7, 'dice'));
SELECT r.id, r.name FROM t_row_alias;
DROP TABLE t_row_alias;
