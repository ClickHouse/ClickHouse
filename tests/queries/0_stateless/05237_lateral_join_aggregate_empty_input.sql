-- A LATERAL subquery that aggregates without GROUP BY yields one row per outer row,
-- also when its filtered input is empty for that outer row (the empty-set aggregate result).

SET enable_analyzer = 1;
SET allow_experimental_lateral_join = 1;
SET join_use_nulls = 1;

DROP TABLE IF EXISTS companies;
DROP TABLE IF EXISTS invoices;

CREATE TABLE companies (id UInt32, region String) ENGINE = MergeTree ORDER BY id;
CREATE TABLE invoices (id UInt32, company_id UInt32, region String, amount Decimal(10, 2), note Nullable(String)) ENGINE = MergeTree ORDER BY (company_id, id);

INSERT INTO companies VALUES (1, 'eu'), (2, 'us'), (3, 'eu');
INSERT INTO invoices VALUES (1, 1, 'eu', 100.00, 'a'), (2, 1, 'eu', 200.00, NULL), (3, 2, 'us', 50.00, 'b'), (4, 2, 'eu', 70.00, 'c');

SELECT '-- INNER JOIN LATERAL keeps the outer row of an empty input';
SELECT c.id, agg.cnt, agg.total
FROM companies c
INNER JOIN LATERAL (SELECT count() AS cnt, sum(amount) AS total FROM invoices i WHERE i.company_id = c.id) AS agg ON true
ORDER BY c.id;

SELECT '-- LEFT JOIN LATERAL joins the empty-set result, not NULLs';
SELECT c.id, agg.cnt, agg.total, agg.avg_amount, agg.max_id, agg.ids, agg.notes
FROM companies c
LEFT JOIN LATERAL (
    SELECT count() AS cnt, sum(amount) AS total, avg(amount) AS avg_amount, max(id) AS max_id, groupArray(id) AS ids, count(note) AS notes
    FROM invoices i WHERE i.company_id = c.id
) AS agg ON true
ORDER BY c.id;

SELECT '-- aggregate arguments using the correlated column';
SELECT c.id, agg.weighted
FROM companies c
LEFT JOIN LATERAL (SELECT sum(amount * c.id) AS weighted FROM invoices i WHERE i.company_id = c.id) AS agg ON true
ORDER BY c.id;

SELECT '-- two correlated columns';
SELECT c.id, c.region, agg.cnt
FROM companies c
INNER JOIN LATERAL (SELECT count() AS cnt FROM invoices i WHERE i.company_id = c.id AND i.region = c.region) AS agg ON true
ORDER BY c.id;

SELECT '-- HAVING over the empty-set result';
SELECT c.id, agg.cnt
FROM companies c
LEFT JOIN LATERAL (SELECT count() AS cnt FROM invoices i WHERE i.company_id = c.id HAVING cnt = 0) AS agg ON true
ORDER BY c.id;

SELECT '-- a user GROUP BY has no row for an empty input';
SELECT c.id, agg.cnt
FROM companies c
LEFT JOIN LATERAL (SELECT company_id, count() AS cnt FROM invoices i WHERE i.company_id = c.id GROUP BY company_id) AS agg ON true
ORDER BY c.id;

SELECT c.id, agg.cnt
FROM companies c
INNER JOIN LATERAL (SELECT company_id, count() AS cnt FROM invoices i WHERE i.company_id = c.id GROUP BY company_id) AS agg ON true
ORDER BY c.id;

SELECT '-- empty_result_for_aggregation_by_empty_set keeps the empty input empty';
SELECT c.id, agg.cnt
FROM companies c
LEFT JOIN LATERAL (SELECT count() AS cnt FROM invoices i WHERE i.company_id = c.id) AS agg ON true
ORDER BY c.id
SETTINGS empty_result_for_aggregation_by_empty_set = 1;

SELECT c.id, agg.cnt
FROM companies c
INNER JOIN LATERAL (SELECT count() AS cnt FROM invoices i WHERE i.company_id = c.id) AS agg ON true
ORDER BY c.id
SETTINGS empty_result_for_aggregation_by_empty_set = 1;

SELECT '-- join_use_nulls = 0';
SELECT c.id, agg.cnt, agg.total
FROM companies c
LEFT JOIN LATERAL (SELECT count() AS cnt, sum(amount) AS total FROM invoices i WHERE i.company_id = c.id) AS agg ON true
ORDER BY c.id
SETTINGS join_use_nulls = 0;

SELECT '-- other decorrelation layouts';
SELECT c.id, agg.cnt
FROM companies c
INNER JOIN LATERAL (SELECT count() AS cnt FROM invoices i WHERE i.company_id = c.id) AS agg ON true
ORDER BY c.id
SETTINGS correlated_subqueries_default_join_kind = 'left';

SELECT c.id, agg.cnt
FROM companies c
INNER JOIN LATERAL (SELECT count() AS cnt FROM invoices i WHERE i.company_id = c.id) AS agg ON true
ORDER BY c.id
SETTINGS correlated_subqueries_use_in_memory_buffer = 0;

SELECT c.id, agg.cnt
FROM companies c
INNER JOIN LATERAL (SELECT count() AS cnt FROM invoices i WHERE i.company_id = c.id) AS agg ON true
ORDER BY c.id
SETTINGS correlated_subqueries_substitute_equivalent_expressions = 0;

DROP TABLE companies;
DROP TABLE invoices;
