-- Reading a subcolumn through a `VIEW` over a `Distributed` table. The trivial view pushdown
-- (`optimize_trivial_view_pushdown_to_distributed`) replaces the view's table expression with the
-- inlined view body, whose projection columns are the view's columns and not their subcolumns, so
-- every subcolumn kind used to fail with `NOT_FOUND_COLUMN_IN_BLOCK`. Such a read now falls back to
-- the regular `StorageView` path, which wraps the body in `SELECT <subcolumn> FROM (<body>)`.

DROP TABLE IF EXISTS t_05196;
DROP TABLE IF EXISTS t_05196_dist;
DROP VIEW IF EXISTS v_05196;

CREATE TABLE t_05196
(
    id UInt64,
    arr Array(UInt32),
    tup Tuple(a UInt32, b String),
    m Map(String, UInt32),
    n Nullable(UInt32),
    j JSON(a UInt32),
    d Dynamic
)
ENGINE = MergeTree ORDER BY id;

INSERT INTO t_05196
SELECT number, [1, 2], (number, 'x'), map('k', number), if(number % 3 = 0, NULL, number), '{"a": 7}', number
FROM numbers(10);

CREATE TABLE t_05196_dist AS t_05196 ENGINE = Distributed(test_shard_localhost, currentDatabase(), t_05196);
CREATE VIEW v_05196 AS SELECT * FROM t_05196_dist;

SELECT 'a plain column', sum(id) FROM v_05196;
SELECT 'Array size0', sum(arr.size0) FROM v_05196;
SELECT 'a Tuple element', sum(tup.a) FROM v_05196;
SELECT 'Map keys', sum(length(m.keys)) FROM v_05196;
SELECT 'Map values', sum(arraySum(m.values)) FROM v_05196;
SELECT 'Nullable null', sum(n.null) FROM v_05196;
SELECT 'a JSON path', sum(j.a) FROM v_05196;
SELECT 'a Dynamic typed subcolumn', sum(d.UInt64) FROM v_05196;
SELECT 'a subcolumn next to a plain column', sum(id), sum(arr.size0) FROM v_05196;
SELECT 'a subcolumn in WHERE', count() FROM v_05196 WHERE tup.a > 5;

-- The same answers as reading the `Distributed` table directly.
SELECT 'directly', sum(arr.size0), sum(tup.a), sum(n.null), sum(j.a), sum(d.UInt64) FROM t_05196_dist;

DROP VIEW v_05196;
DROP TABLE t_05196_dist;
DROP TABLE t_05196;
