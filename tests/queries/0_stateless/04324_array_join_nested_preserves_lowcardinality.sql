-- Test ARRAY JOIN on a real Nested column with a LowCardinality member preserves LowCardinality
-- in the result type and values (internally resolved via the nested() function). Issue #95582.

DROP TABLE IF EXISTS t_aj_nested_lc;

CREATE TABLE t_aj_nested_lc
(
    a LowCardinality(String),
    foos Nested(x LowCardinality(String), y UInt32)
)
ENGINE = MergeTree ORDER BY a;

INSERT INTO t_aj_nested_lc VALUES ('r1', ['p', 'q'], [1, 2]) ('r2', ['p'], [3]);

SELECT 'array-joined subcolumn type';
SELECT DISTINCT toTypeName(foo.x) FROM t_aj_nested_lc ARRAY JOIN foos AS foo;

SELECT 'array-joined whole-tuple type';
SELECT DISTINCT toTypeName(foo) FROM t_aj_nested_lc ARRAY JOIN foos AS foo;

SELECT 'array-joined values';
SELECT a, foo.x, foo.y FROM t_aj_nested_lc ARRAY JOIN foos AS foo ORDER BY a, foo.x, foo.y;

DROP TABLE t_aj_nested_lc;
