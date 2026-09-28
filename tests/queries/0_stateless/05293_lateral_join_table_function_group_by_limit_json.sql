-- Tags: no-fasttest
-- no-fasttest: `formatQueryFromJSON` is not built in the fast test.

-- Regression tests for three `LATERAL JOIN` review findings:
--
--  1. `lateral(...)` that is not a subquery is a table function call, not the `LATERAL` keyword.
--  2. The user's `max_rows_to_group_by` / `group_by_overflow_mode` do not apply to the grouping by
--     the correlated columns that decorrelation adds.
--  3. A `clickhouse_json` payload cannot build a `LATERAL` join without `ON`/`USING`.

SELECT '-- (1)';
SELECT formatQuery('SELECT * FROM t JOIN lateral(1, 2) ON true');
SELECT formatQuery('SELECT * FROM t JOIN lateral(numbers(3)) AS l ON t.id = l.number');
SELECT formatQuery('SELECT * FROM t, lateral(1)');
SELECT formatQuery('SELECT * FROM t JOIN LATERAL (SELECT 1) AS s ON true');
SELECT formatQuery('SELECT * FROM t LEFT JOIN lateral ((SELECT 1)) AS s ON true');

SET allow_experimental_lateral_join = 1;

DROP TABLE IF EXISTS outer_t;
DROP TABLE IF EXISTS inner_t;

CREATE TABLE outer_t (id UInt32, k UInt32) ENGINE = MergeTree ORDER BY id;
CREATE TABLE inner_t (k UInt32, v UInt32) ENGINE = MergeTree ORDER BY v;

INSERT INTO outer_t VALUES (1, 1), (2, 2), (3, 3), (4, 1);
INSERT INTO inner_t VALUES (1, 10), (1, 11), (2, 20), (5, 50);

SELECT '-- (2) throw';
SELECT o.id, agg.cnt
FROM outer_t AS o
LEFT JOIN LATERAL (SELECT count() AS cnt FROM inner_t AS i WHERE i.k = o.k) AS agg ON true
ORDER BY o.id
SETTINGS max_rows_to_group_by = 1, group_by_overflow_mode = 'throw';

SELECT '-- (2) any';
SELECT o.id, agg.cnt, agg.s
FROM outer_t AS o
INNER JOIN LATERAL (SELECT count() AS cnt, sum(i.v) AS s FROM inner_t AS i WHERE i.k = o.k) AS agg ON true
ORDER BY o.id
SETTINGS max_rows_to_group_by = 1, group_by_overflow_mode = 'any';

SELECT '-- (2) user GROUP BY';
SELECT o.id, sub.v, sub.cnt
FROM outer_t AS o
INNER JOIN LATERAL (SELECT i.v, count() AS cnt FROM inner_t AS i WHERE i.k = o.k GROUP BY i.v) AS sub ON true
ORDER BY o.id, sub.v
SETTINGS max_rows_to_group_by = 1, group_by_overflow_mode = 'any';

DROP TABLE outer_t;
DROP TABLE inner_t;

SELECT '-- (3)';
-- A `lateral` join without `ON`/`USING` is rejected:
SELECT formatQueryFromJSON('{"type":"SelectWithUnionQuery","union_mode":"UNION_DEFAULT","list_of_selects":{"type":"ExpressionList","children":[{"type":"SelectQuery","select":{"type":"ExpressionList","children":[{"type":"Asterisk"}]},"tables":{"type":"TablesInSelectQuery","children":[{"type":"TablesInSelectQueryElement","table_expression":{"type":"TableExpression","database_and_table_name":{"type":"TableIdentifier","name":"t1"}}},{"type":"TablesInSelectQueryElement","table_join":{"type":"TableJoin","kind":"INNER","lateral":true},"table_expression":{"type":"TableExpression","subquery":{"type":"Subquery","alias":"s","children":[{"type":"SelectWithUnionQuery","union_mode":"UNION_DEFAULT","list_of_selects":{"type":"ExpressionList","children":[{"type":"SelectQuery","select":{"type":"ExpressionList","children":[{"type":"Literal","value":{"field_type":"UInt64","value":1}}]}}]},"is_outfile_append":false,"is_outfile_truncate":false,"is_into_outfile_with_stdout":false}]}}}]}}]},"is_outfile_append":false,"is_outfile_truncate":false,"is_into_outfile_with_stdout":false}'); -- { serverError BAD_ARGUMENTS }
-- The same payload with `ON true` is accepted:
SELECT formatQueryFromJSON(parseQueryToJSON('SELECT * FROM t1 INNER JOIN LATERAL (SELECT 1) AS s ON true'));
