-- Reading a subcolumn through a `VIEW` over a `Distributed` table. The trivial view pushdown
-- (`optimize_trivial_view_pushdown_to_distributed`) replaces the view's table expression with the
-- inlined view body, whose projection columns are the view's columns and not their subcolumns, so
-- every subcolumn kind used to fail with `NOT_FOUND_COLUMN_IN_BLOCK`. Such a read now falls back to
-- the regular `StorageView` path, which wraps the body in `SELECT <subcolumn> FROM (<body>)`.

DROP TABLE IF EXISTS t_05227;
DROP TABLE IF EXISTS t_05227_dist;
DROP VIEW IF EXISTS v_05227;

CREATE TABLE t_05227
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

INSERT INTO t_05227
SELECT number, [1, 2], (number, 'x'), map('k', number), if(number % 3 = 0, NULL, number), '{"a": 7}', number
FROM numbers(10);

CREATE TABLE t_05227_dist AS t_05227 ENGINE = Distributed(test_shard_localhost, currentDatabase(), t_05227);
CREATE VIEW v_05227 AS SELECT * FROM t_05227_dist;

SELECT 'a plain column', sum(id) FROM v_05227;
SELECT 'Array size0', sum(arr.size0) FROM v_05227;
SELECT 'a Tuple element', sum(tup.a) FROM v_05227;
SELECT 'Map keys', sum(length(m.keys)) FROM v_05227;
SELECT 'Map values', sum(arraySum(m.values)) FROM v_05227;
SELECT 'Nullable null', sum(n.null) FROM v_05227;
SELECT 'a JSON path', sum(j.a) FROM v_05227;
SELECT 'a Dynamic typed subcolumn', sum(d.UInt64) FROM v_05227;
SELECT 'a subcolumn next to a plain column', sum(id), sum(arr.size0) FROM v_05227;
SELECT 'a subcolumn in WHERE', count() FROM v_05227 WHERE tup.a > 5;

-- A subcolumn referenced only from a view-keyed `additional_table_filters` entry. The filter is
-- built against the view before the pushdown gate runs and registers its columns with the table
-- expression, so the subcolumn reaches the same check as a directly read one.
SELECT 'a subcolumn in additional_table_filters', count() FROM v_05227
SETTINGS additional_table_filters = {'v_05227': 'arr.size0 > 0'};
SELECT 'a Tuple element in additional_table_filters', sum(id) FROM v_05227
SETTINGS additional_table_filters = {'v_05227': 'tup.a > 5'};
SELECT 'a JSON path in additional_table_filters', count() FROM v_05227
SETTINGS additional_table_filters = {'v_05227': 'j.a = 7'};
SELECT 'a Dynamic typed subcolumn in additional_table_filters', count() FROM v_05227
SETTINGS additional_table_filters = {'v_05227': 'd.UInt64 < 3'};

-- Pushdown is suppressed for the filter case, the same as for a direct subcolumn read: the plan keeps
-- the `StorageView` conversion step. A plain-column filter keeps the pushdown.
SET explain_query_plan_default = 'legacy';
SELECT 'pushdown suppressed for a subcolumn filter', countIf(explain LIKE '%VIEW subquery%') > 0
FROM (EXPLAIN SELECT count() FROM v_05227 SETTINGS additional_table_filters = {'v_05227': 'arr.size0 > 0'});
SELECT 'pushdown suppressed for a direct subcolumn read', countIf(explain LIKE '%VIEW subquery%') > 0
FROM (EXPLAIN SELECT sum(arr.size0) FROM v_05227);
SELECT 'pushdown kept for a plain column filter', countIf(explain LIKE '%VIEW subquery%') = 0
FROM (EXPLAIN SELECT count() FROM v_05227 SETTINGS additional_table_filters = {'v_05227': 'id > 0'});

-- The same answers as reading the `Distributed` table directly.
SELECT 'directly', sum(arr.size0), sum(tup.a), sum(n.null), sum(j.a), sum(d.UInt64) FROM t_05227_dist;

DROP VIEW v_05227;
DROP TABLE t_05227_dist;
DROP TABLE t_05227;
