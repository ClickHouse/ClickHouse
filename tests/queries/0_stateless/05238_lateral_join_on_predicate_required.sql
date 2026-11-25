-- The `ON` predicate of `JOIN LATERAL` cannot be omitted: the parser requires `ON` or `USING`
-- for every non-CROSS join, and the documented grammar advertises exactly `ON true`.
SET allow_experimental_lateral_join = 1;

DROP TABLE IF EXISTS companies;
DROP TABLE IF EXISTS invoices;
CREATE TABLE companies (id UInt32, name String) ENGINE = Memory;
CREATE TABLE invoices (id UInt32, company_id UInt32, amount UInt32) ENGINE = Memory;
INSERT INTO companies VALUES (1, 'Acme'), (2, 'Globex');
INSERT INTO invoices VALUES (10, 1, 100), (11, 1, 50);

SELECT c.id, inv.id
FROM companies AS c
LEFT JOIN LATERAL (SELECT * FROM invoices AS i WHERE i.company_id = c.id) AS inv; -- { clientError SYNTAX_ERROR }

SELECT c.id, inv.id
FROM companies AS c
INNER JOIN LATERAL (SELECT * FROM invoices AS i WHERE i.company_id = c.id) AS inv; -- { clientError SYNTAX_ERROR }

-- The same query with the mandatory `ON true` works.
SELECT c.id, inv.id
FROM companies AS c
LEFT JOIN LATERAL (SELECT * FROM invoices AS i WHERE i.company_id = c.id) AS inv ON true
ORDER BY c.id, inv.id;

-- A correlated table expression in the FROM clause that is not the right side of `JOIN LATERAL`
-- is rejected: the diagnostic describes the supported subset instead of denying the feature.
SELECT c.id, inv.id
FROM companies AS c
LEFT JOIN (SELECT * FROM invoices AS i WHERE i.company_id = c.id) AS inv ON true
ORDER BY c.id, inv.id; -- { serverError NOT_IMPLEMENTED }

DROP TABLE companies;
DROP TABLE invoices;
