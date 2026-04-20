-- Tags: no-fasttest
-- Test LATERAL JOIN support

SET enable_analyzer = 1;
SET allow_experimental_correlated_subqueries = 1;
SET allow_experimental_lateral_join = 1;
SET join_use_nulls = 1;

-- Setup test tables
DROP TABLE IF EXISTS companies;
DROP TABLE IF EXISTS invoices;

CREATE TABLE companies (id UInt32, name String) ENGINE = MergeTree ORDER BY id;
CREATE TABLE invoices (id UInt32, company_id UInt32, amount Decimal(10,2), date Date) ENGINE = MergeTree ORDER BY (company_id, date);

INSERT INTO companies VALUES (1, 'Acme'), (2, 'Globex'), (3, 'Initech');
INSERT INTO invoices VALUES (1, 1, 100.00, '2024-01-15'), (2, 1, 200.00, '2024-06-20'), (3, 2, 50.00, '2024-03-10'), (4, 1, 150.00, '2024-03-01');

-- Test 1: Basic LEFT JOIN LATERAL with correlated WHERE
SELECT '-- Test 1: Basic LEFT JOIN LATERAL';
SELECT c.id, c.name, inv.id as inv_id, inv.amount, inv.date
FROM companies c
LEFT JOIN LATERAL (
    SELECT * FROM invoices i
    WHERE i.company_id = c.id
) AS inv ON true
ORDER BY c.id, inv.id;

-- Test 2: INNER JOIN LATERAL
SELECT '-- Test 2: INNER JOIN LATERAL';
SELECT c.id, c.name, inv.id as inv_id, inv.amount
FROM companies c
INNER JOIN LATERAL (
    SELECT * FROM invoices i
    WHERE i.company_id = c.id
) AS inv ON true
ORDER BY c.id, inv.id;

-- Test 3: LEFT JOIN LATERAL with ORDER BY + LIMIT 1 (the canonical "latest row" pattern)
SELECT '-- Test 3: LEFT JOIN LATERAL with ORDER BY LIMIT 1';
SELECT c.id, c.name, inv.id as inv_id, inv.amount, inv.date
FROM companies c
LEFT JOIN LATERAL (
    SELECT * FROM invoices i
    WHERE i.company_id = c.id
    ORDER BY i.date DESC
    LIMIT 1
) AS inv ON true
ORDER BY c.id;

-- Test 4: LEFT JOIN LATERAL with aggregation inside the subquery
SELECT '-- Test 4: LEFT JOIN LATERAL with aggregation';
SELECT c.id, c.name, inv.total_amount, inv.invoice_count
FROM companies c
LEFT JOIN LATERAL (
    SELECT
        sum(amount) AS total_amount,
        count() AS invoice_count
    FROM invoices i
    WHERE i.company_id = c.id
) AS inv ON true
ORDER BY c.id;

-- Test 5: LEFT JOIN LATERAL with ORDER BY + LIMIT 1 OFFSET 1 (skip the latest, get the second)
SELECT '-- Test 5: LEFT JOIN LATERAL with LIMIT 1 OFFSET 1';
SELECT c.id, c.name, inv.id as inv_id, inv.amount, inv.date
FROM companies c
LEFT JOIN LATERAL (
    SELECT * FROM invoices i
    WHERE i.company_id = c.id
    ORDER BY i.date DESC
    LIMIT 1 OFFSET 1
) AS inv ON true
ORDER BY c.id;

-- Test 5b: Error case - LATERAL with LIMIT ... WITH TIES is not supported
SELECT c.id
FROM companies c
LEFT JOIN LATERAL (
    SELECT * FROM invoices i
    WHERE i.company_id = c.id
    ORDER BY i.date DESC
    LIMIT 1 WITH TIES
) AS inv ON true; -- { serverError NOT_IMPLEMENTED }

-- Test 5c: Error case - non-correlated LATERAL subquery
SELECT c.id
FROM companies c
LEFT JOIN LATERAL (
    SELECT 1 AS x
) AS inv ON true; -- { serverError NOT_IMPLEMENTED }

SELECT c.id FROM companies c CROSS JOIN LATERAL (SELECT * FROM invoices i WHERE i.company_id = c.id) AS inv; -- { clientError SYNTAX_ERROR } -- CROSS JOIN LATERAL not supported

-- Test 6: Error case - LATERAL without experimental setting (correlated subquery)
SELECT '-- Test 6: Error without setting';
SET allow_experimental_lateral_join = 0;
SELECT c.id
FROM companies c
LEFT JOIN LATERAL (
    SELECT * FROM invoices i
    WHERE i.company_id = c.id
) AS inv ON true; -- { serverError SUPPORT_IS_DISABLED }

-- Test 6b: Error case - LATERAL without experimental setting (non-correlated subquery)
SELECT c.id
FROM companies c
LEFT JOIN LATERAL (
    SELECT 1 AS x
) AS inv ON true; -- { serverError SUPPORT_IS_DISABLED }
SET allow_experimental_lateral_join = 1;

-- Test 7: Error case - RIGHT JOIN LATERAL is not supported
SELECT c.id
FROM companies c
RIGHT JOIN LATERAL (
    SELECT * FROM invoices i
    WHERE i.company_id = c.id
) AS inv ON true; -- { serverError NOT_IMPLEMENTED }

-- Test 8: Error case - FULL JOIN LATERAL is not supported
SELECT c.id
FROM companies c
FULL JOIN LATERAL (
    SELECT * FROM invoices i
    WHERE i.company_id = c.id
) AS inv ON true; -- { serverError NOT_IMPLEMENTED }

-- Test 9: Error case - LATERAL with non-trivial ON predicate
SELECT c.id
FROM companies c
LEFT JOIN LATERAL (
    SELECT * FROM invoices i
    WHERE i.company_id = c.id
) AS inv ON inv.amount > 100; -- { serverError NOT_IMPLEMENTED }

-- Test 10: Error case - LATERAL with USING clause
SELECT c.id
FROM companies c
LEFT JOIN LATERAL (
    SELECT c.id as id, amount FROM invoices i
    WHERE i.company_id = c.id
) AS inv USING (id); -- { serverError NOT_IMPLEMENTED }

-- Test 11: Error case - LEFT SEMI JOIN LATERAL is not supported
SELECT c.id
FROM companies c
LEFT SEMI JOIN LATERAL (
    SELECT * FROM invoices i
    WHERE i.company_id = c.id
) AS inv ON true; -- { serverError NOT_IMPLEMENTED }

-- Test 12: Error case - LEFT ANTI JOIN LATERAL is not supported
SELECT c.id
FROM companies c
LEFT ANTI JOIN LATERAL (
    SELECT * FROM invoices i
    WHERE i.company_id = c.id
) AS inv ON true; -- { serverError NOT_IMPLEMENTED }

-- Test 13: Error case - ASOF JOIN LATERAL is not supported
SELECT c.id
FROM companies c
ASOF JOIN LATERAL (
    SELECT * FROM invoices i
    WHERE i.company_id = c.id
) AS inv ON c.id = inv.company_id AND c.id >= inv.id; -- { serverError NOT_IMPLEMENTED }

-- Test 14: Error case - NATURAL LEFT JOIN LATERAL is not supported
SELECT c.id
FROM companies c
NATURAL LEFT JOIN LATERAL (
    SELECT * FROM invoices i
    WHERE i.company_id = c.id
) AS inv; -- { serverError NOT_IMPLEMENTED }

-- Cleanup
DROP TABLE IF EXISTS companies;
DROP TABLE IF EXISTS invoices;
