-- Tags: no-fasttest

DROP TABLE IF EXISTS test_combinators;

CREATE TABLE test_combinators (
    country String,
    city String,
    department String,
    salary UInt64
) ENGINE = MergeTree() ORDER BY (country, city);

INSERT INTO test_combinators VALUES
    ('RU', 'Moscow', 'Engineering', 120000),
    ('RU', 'Moscow', 'Sales', 100000),
    ('RU', 'SPb', 'Engineering', 110000),
    ('NL', 'Amsterdam', 'Engineering', 95000),
    ('NL', 'Amsterdam', 'Sales', 80000);

SELECT '--- T1: TOTALS basic ---';
SELECT country, round(avg(salary)) AS a, round(avg(salary TOTALS)) AS t
FROM test_combinators GROUP BY country ORDER BY country;

SELECT '--- T2: TOTALS multi-aggregate (FuseFunctionsPass test) ---';
SELECT country, sum(salary TOTALS) AS s, round(avg(salary TOTALS)) AS a
FROM test_combinators GROUP BY country ORDER BY country;

SELECT '--- T3: BY single column ---';
SELECT country, city, round(avg(salary)) AS local, round(avg(salary BY country)) AS by_c
FROM test_combinators GROUP BY country, city ORDER BY country, city;

SELECT '--- T4: BY multiple columns ---';
SELECT country, city, department,
       round(avg(salary)) AS local,
       round(avg(salary BY country, department)) AS by_cd
FROM test_combinators GROUP BY country, city, department ORDER BY country, city, department;

SELECT '--- T5: Mixed TOTALS and BY ---';
SELECT country, city,
       round(avg(salary)) AS local,
       round(avg(salary BY country)) AS by_c,
       round(avg(salary TOTALS)) AS tot
FROM test_combinators GROUP BY country, city ORDER BY country, city;

SELECT '--- T6: BY equals GROUP BY (degenerate) ---';
SELECT country, city, round(avg(salary)) AS a, round(avg(salary BY country, city)) AS b
FROM test_combinators GROUP BY country, city ORDER BY country, city;

SELECT '--- T7: Identifier with name "totals" (backward compatibility) ---';
DROP TABLE IF EXISTS test_identifiers;
CREATE TABLE test_identifiers (totals UInt64) ENGINE = MergeTree() ORDER BY tuple();
INSERT INTO test_identifiers VALUES (1), (2), (3);
SELECT count(totals), sum(totals) FROM test_identifiers;
DROP TABLE test_identifiers;

SELECT '--- T8: Validation - BY column not in GROUP BY ---';
SELECT country, avg(salary BY department) FROM test_combinators GROUP BY country; -- { serverError BAD_ARGUMENTS }

SELECT '--- T9: Validation - unknown column in BY ---';
SELECT country, avg(salary BY unknown) FROM test_combinators GROUP BY country; -- { serverError UNKNOWN_IDENTIFIER }

SELECT '--- T10: Validation - duplicate TOTALS ---';
SELECT avg(salary TOTALS TOTALS) FROM test_combinators GROUP BY country; -- { clientError SYNTAX_ERROR }

SELECT '--- T11: Validation - duplicate BY ---';
SELECT avg(salary BY country BY city) FROM test_combinators GROUP BY country, city; -- { clientError SYNTAX_ERROR }

SELECT '--- T12: Validation - TOTALS + BY in same call ---';
SELECT avg(salary TOTALS BY country) FROM test_combinators GROUP BY country; -- { clientError SYNTAX_ERROR }

DROP TABLE test_combinators;