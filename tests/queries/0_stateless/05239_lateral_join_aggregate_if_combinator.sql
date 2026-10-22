-- A LATERAL subquery aggregating without GROUP BY restores the empty-set row for an outer row
-- with an empty input. Aggregates that already have the -If combinator must keep their own filter.

SET enable_analyzer = 1;
SET allow_experimental_lateral_join = 1;

DROP TABLE IF EXISTS companies;
DROP TABLE IF EXISTS invoices;

CREATE TABLE companies (id UInt32) ENGINE = MergeTree ORDER BY id;
CREATE TABLE invoices (id UInt32, company_id UInt32, amount UInt64, note Nullable(String)) ENGINE = MergeTree ORDER BY (company_id, id);

INSERT INTO companies VALUES (1), (2), (3);
INSERT INTO invoices VALUES (1, 1, 100, 'a'), (2, 1, 200, NULL), (3, 2, 50, 'b'), (4, 2, 70, 'c'), (5, 2, 80, 'a');

SELECT '-- -If aggregates';
SELECT c.id, agg.*
FROM companies c
LEFT JOIN LATERAL (
    SELECT
        countIf(amount > 60) AS cnt,
        sumIf(amount, amount > 60) AS total,
        sumIf(toNullable(amount), amount > 60) AS nullable_total,
        uniqExactIf(note, amount > 60) AS notes,
        groupArrayIf(id, amount > 60) AS ids,
        countIf(note = 'a') AS nullable_filter,
        sumIfOrNull(amount, amount > 60) AS or_null,
        count() AS all_rows
    FROM invoices i WHERE i.company_id = c.id
) AS agg ON true
ORDER BY c.id;

SELECT '-- the same aggregates over an empty input without LATERAL';
SELECT countIf(amount > 60), sumIf(amount, amount > 60), sumIf(toNullable(amount), amount > 60), uniqExactIf(note, amount > 60),
    groupArrayIf(id, amount > 60), countIf(note = 'a'), sumIfOrNull(amount, amount > 60), count()
FROM invoices WHERE company_id = 3;

SELECT '-- INNER JOIN LATERAL';
SELECT c.id, agg.cnt, agg.total
FROM companies c
INNER JOIN LATERAL (SELECT countIf(amount > 60) AS cnt, sumIf(amount, amount > 60) AS total FROM invoices i WHERE i.company_id = c.id) AS agg ON true
ORDER BY c.id;

SELECT '-- aggregates with Nullable arguments and -OrNull';
SELECT c.id, agg.*
FROM companies c
LEFT JOIN LATERAL (
    SELECT
        countOrNull(note) AS notes_or_null,
        count(note) AS notes,
        sumOrNull(toNullable(amount)) AS nullable_total,
        maxOrDefault(note) AS max_note,
        any(note) RESPECT NULLS AS any_note,
        argMax(note, id) AS last_note,
        quantileTDigest(id) AS median_id,
        quantile(0.5)(amount) AS median_amount
    FROM invoices i WHERE i.company_id = c.id
) AS agg ON true
ORDER BY c.id;

SELECT '-- the same aggregates over an empty input without LATERAL';
SELECT countOrNull(note), count(note), sumOrNull(toNullable(amount)), maxOrDefault(note), any(note) RESPECT NULLS, argMax(note, id),
    quantileTDigest(id), quantile(0.5)(amount)
FROM invoices WHERE company_id = 3;

DROP TABLE companies;
DROP TABLE invoices;
