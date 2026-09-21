-- A `MATERIALIZED` CTE declared inside a lightweight `UPDATE` must be materialized once, like in a
-- `SELECT`, and its references must stay references: they are not table names.
-- https://github.com/ClickHouse/ClickHouse/issues/113711
-- `same` compares rand64() taken from two references of one CTE: 1 = one materialization.

SET enable_materialized_cte = 1;
SET enable_lightweight_update = 1;

DROP TABLE IF EXISTS t_lwu_113711b, c_lwu_113711b;

CREATE TABLE t_lwu_113711b (id UInt64, same UInt8) ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;
INSERT INTO t_lwu_113711b SELECT number, 0 FROM numbers(3);

SELECT '-- two references in an assignment read one materialization';
UPDATE t_lwu_113711b SET same = (WITH c_lwu_113711b AS MATERIALIZED (SELECT rand64() AS x) SELECT count() FROM c_lwu_113711b AS a, c_lwu_113711b AS b WHERE a.x = b.x) WHERE 1;
SELECT DISTINCT same FROM t_lwu_113711b;

SELECT '-- a table of the CTE name does not capture the reference in an IN subquery';
-- The rows of this table match nothing in `t_lwu_113711b`, so binding to it would update no row.
CREATE TABLE c_lwu_113711b (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO c_lwu_113711b VALUES (100), (200);
UPDATE t_lwu_113711b SET same = 2 WHERE id IN (WITH c_lwu_113711b AS MATERIALIZED (SELECT number AS id FROM numbers(2)) SELECT id FROM c_lwu_113711b);
SELECT id, same FROM t_lwu_113711b ORDER BY id;

DROP TABLE c_lwu_113711b, t_lwu_113711b;
