-- Tags: no-fasttest

-- Regression tests for two `LATERAL JOIN` review findings:
--
--  1. `ASTTableJoin::lateral` must survive the `clickhouse_json` AST round-trip, and a
--     JSON payload must not be able to build a `LATERAL` join shape that the SQL parser
--     rejects (`CROSS`/comma joins).
--
--  2. `GLOBAL JOIN LATERAL` must be rejected: the decorrelated plan does not carry the
--     locality over, so it would silently run as an ordinary local join.

SET allow_experimental_lateral_join = 1;

-- (1) The `LATERAL` flag round-trips through the AST JSON:
SELECT formatQueryFromJSON(parseQueryToJSON('SELECT * FROM companies AS c LEFT JOIN LATERAL (SELECT * FROM invoices AS i WHERE i.company_id = c.id) AS inv ON true'));
SELECT formatQueryFromJSON(parseQueryToJSON('SELECT * FROM companies AS c INNER JOIN LATERAL (SELECT * FROM invoices AS i WHERE i.company_id = c.id) AS inv ON true'));

-- (1) A `CROSS JOIN` carrying `lateral` is a shape the parser can never produce and is rejected:
SELECT formatQueryFromJSON('{"type":"SelectWithUnionQuery","children":[{"type":"ExpressionList","children":[{"type":"SelectQuery","select":{"type":"ExpressionList","children":[{"type":"Asterisk"}]},"tables":{"type":"TablesInSelectQuery","children":[{"type":"TablesInSelectQueryElement","table_expression":{"type":"TableExpression","database_and_table_name":{"type":"Identifier","name":"t1"}}},{"type":"TablesInSelectQueryElement","table_join":{"type":"TableJoin","kind":"CROSS","lateral":true},"table_expression":{"type":"TableExpression","database_and_table_name":{"type":"Identifier","name":"t2"}}}]}}]}]}'); -- { serverError BAD_ARGUMENTS }

DROP TABLE IF EXISTS companies;
DROP TABLE IF EXISTS invoices;

CREATE TABLE companies (id UInt32, name String) ENGINE = MergeTree ORDER BY id;
CREATE TABLE invoices (id UInt32, company_id UInt32) ENGINE = MergeTree ORDER BY (company_id, id);

INSERT INTO companies VALUES (1, 'Acme'), (2, 'Globex');
INSERT INTO invoices VALUES (1, 1), (2, 1), (3, 2);

-- (2) `GLOBAL JOIN LATERAL` is rejected instead of silently dropping the locality:
SELECT c.id, inv.id
FROM companies AS c
GLOBAL LEFT JOIN LATERAL (SELECT * FROM invoices AS i WHERE i.company_id = c.id) AS inv ON true
ORDER BY ALL; -- { serverError NOT_IMPLEMENTED }

-- The same query without `GLOBAL` works:
SELECT c.id, inv.id
FROM companies AS c
LEFT JOIN LATERAL (SELECT * FROM invoices AS i WHERE i.company_id = c.id) AS inv ON true
ORDER BY ALL;

DROP TABLE companies;
DROP TABLE invoices;
