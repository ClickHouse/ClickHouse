-- Tags: no-fasttest
-- no-fasttest: `parseQueryToJSON` / `formatQueryFromJSON` are not built in the fast test.

-- Regression tests for three `LATERAL JOIN` review findings:
--
--  1. A `NULL` correlated value is its own domain tuple, so the outer row must get the result of
--     the subquery evaluated for it (`count()` = 0), not be null-extended or dropped.
--  2. An explicit `LOCAL` locality is rejected like `GLOBAL`, instead of being silently dropped.
--  3. A `clickhouse_json` payload cannot put `lateral` on a join whose right side is not a subquery.

SET allow_experimental_lateral_join = 1;
-- The not aggregating case fills the unmatched outer rows with defaults.
SET join_use_nulls = 0;

DROP TABLE IF EXISTS outer_t;
DROP TABLE IF EXISTS inner_t;

CREATE TABLE outer_t (id UInt32, k Nullable(UInt32)) ENGINE = MergeTree ORDER BY id;
CREATE TABLE inner_t (k Nullable(UInt32), v UInt32) ENGINE = MergeTree ORDER BY v;

INSERT INTO outer_t VALUES (1, 1), (2, NULL), (3, 3);
INSERT INTO inner_t VALUES (1, 10), (1, 11), (NULL, 20);

SELECT '-- (1) LEFT, aggregate';
SELECT o.id, o.k, agg.cnt, agg.s
FROM outer_t AS o
LEFT JOIN LATERAL (SELECT count() AS cnt, sum(i.v) AS s FROM inner_t AS i WHERE i.k = o.k) AS agg ON true
ORDER BY o.id;

SELECT '-- (1) INNER, aggregate';
SELECT o.id, o.k, agg.cnt
FROM outer_t AS o
INNER JOIN LATERAL (SELECT count() AS cnt FROM inner_t AS i WHERE i.k = o.k) AS agg ON true
ORDER BY o.id;

SELECT '-- (1) the subquery sees the NULL outer value';
SELECT o.id, agg.cnt
FROM outer_t AS o
INNER JOIN LATERAL (SELECT count() AS cnt FROM inner_t AS i WHERE i.k = o.k OR (o.k IS NULL AND i.k IS NULL)) AS agg ON true
ORDER BY o.id;

SELECT '-- (1) LEFT, not aggregating';
SELECT o.id, sub.v
FROM outer_t AS o
LEFT JOIN LATERAL (SELECT i.v FROM inner_t AS i WHERE i.k = o.k) AS sub ON true
ORDER BY o.id, sub.v;

-- (2) `LOCAL JOIN LATERAL` is rejected instead of silently dropping the locality:
SELECT o.id, agg.cnt
FROM outer_t AS o
LOCAL LEFT JOIN LATERAL (SELECT count() AS cnt FROM inner_t AS i WHERE i.k = o.k) AS agg ON true
ORDER BY o.id; -- { serverError NOT_IMPLEMENTED }

-- (3) `lateral` on a join with a plain table on the right side is rejected:
SELECT formatQueryFromJSON('{"type":"SelectWithUnionQuery","union_mode":"UNION_DEFAULT","list_of_selects":{"type":"ExpressionList","children":[{"type":"SelectQuery","select":{"type":"ExpressionList","children":[{"type":"Asterisk"}]},"tables":{"type":"TablesInSelectQuery","children":[{"type":"TablesInSelectQueryElement","table_expression":{"type":"TableExpression","database_and_table_name":{"type":"TableIdentifier","name":"t1"}}},{"type":"TablesInSelectQueryElement","table_join":{"type":"TableJoin","kind":"INNER","lateral":true,"on_expression":{"type":"Literal","value":{"field_type":"Bool","value":true}}},"table_expression":{"type":"TableExpression","database_and_table_name":{"type":"TableIdentifier","name":"t2"}}}]}}]}}'); -- { serverError BAD_ARGUMENTS }
-- The same payload without `lateral` is accepted:
SELECT formatQueryFromJSON('{"type":"SelectWithUnionQuery","union_mode":"UNION_DEFAULT","list_of_selects":{"type":"ExpressionList","children":[{"type":"SelectQuery","select":{"type":"ExpressionList","children":[{"type":"Asterisk"}]},"tables":{"type":"TablesInSelectQuery","children":[{"type":"TablesInSelectQueryElement","table_expression":{"type":"TableExpression","database_and_table_name":{"type":"TableIdentifier","name":"t1"}}},{"type":"TablesInSelectQueryElement","table_join":{"type":"TableJoin","kind":"INNER","on_expression":{"type":"Literal","value":{"field_type":"Bool","value":true}}},"table_expression":{"type":"TableExpression","database_and_table_name":{"type":"TableIdentifier","name":"t2"}}}]}}]}}');

-- The `lateral` flag with a subquery on the right side still round-trips:
SELECT formatQueryFromJSON(parseQueryToJSON('SELECT * FROM t1 INNER JOIN LATERAL (SELECT * FROM t2 WHERE t2.k = t1.k) AS s ON true'));

DROP TABLE outer_t;
DROP TABLE inner_t;
