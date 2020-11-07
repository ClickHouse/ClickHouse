DROP TABLE IF EXISTS columns_transformers;

CREATE TABLE columns_transformers (i Int64, j Int16, k Int64) Engine=TinyLog;
INSERT INTO columns_transformers VALUES (100, 10, 324), (120, 8, 23);

SELECT * APPLY(sum) from columns_transformers;
SELECT columns_transformers.* APPLY(avg) from columns_transformers;
SELECT a.* APPLY(toDate) APPLY(any) from columns_transformers a;
SELECT COLUMNS('[jk]') APPLY(toString) APPLY(length) from columns_transformers;

SELECT * EXCEPT(i) APPLY(sum) from columns_transformers;
SELECT columns_transformers.* EXCEPT(j) APPLY(avg) from columns_transformers;
-- EXCEPT after APPLY will not match anything
SELECT a.* APPLY(toDate) EXCEPT(i, j) APPLY(any) from columns_transformers a;

SELECT * REPLACE(i + 1 AS i) APPLY(sum) from columns_transformers;
SELECT columns_transformers.* REPLACE(j + 2 AS j, i + 1 AS i) APPLY(avg) from columns_transformers;
SELECT columns_transformers.* REPLACE(j + 1 AS j, j + 2 AS j) APPLY(avg) from columns_transformers; -- { serverError 43 }
-- REPLACE after APPLY will not match anything
SELECT a.* APPLY(toDate) REPLACE(i + 1 AS i) APPLY(any) from columns_transformers a;

EXPLAIN SYNTAX SELECT * APPLY(sum) from columns_transformers;
EXPLAIN SYNTAX SELECT columns_transformers.* APPLY(avg) from columns_transformers;
EXPLAIN SYNTAX SELECT a.* APPLY(toDate) APPLY(any) from columns_transformers a;
EXPLAIN SYNTAX SELECT COLUMNS('[jk]') APPLY(toString) APPLY(length) from columns_transformers;
EXPLAIN SYNTAX SELECT * EXCEPT(i) APPLY(sum) from columns_transformers;
EXPLAIN SYNTAX SELECT columns_transformers.* EXCEPT(j) APPLY(avg) from columns_transformers;
EXPLAIN SYNTAX SELECT a.* APPLY(toDate) EXCEPT(i, j) APPLY(any) from columns_transformers a;
EXPLAIN SYNTAX SELECT * REPLACE(i + 1 AS i) APPLY(sum) from columns_transformers;
EXPLAIN SYNTAX SELECT columns_transformers.* REPLACE(j + 2 AS j, i + 1 AS i) APPLY(avg) from columns_transformers;
EXPLAIN SYNTAX SELECT a.* APPLY(toDate) REPLACE(i + 1 AS i) APPLY(any) from columns_transformers a;

-- Multiple REPLACE in a row
EXPLAIN SYNTAX SELECT * REPLACE(i + 1 AS i) REPLACE(i + 1 AS i) from columns_transformers;

-- Explicit column list
SELECT COLUMNS(i, j, k) APPLY(sum) from columns_transformers;
EXPLAIN SYNTAX SELECT COLUMNS(i, j, k) APPLY(sum) from columns_transformers;

-- Multiple column matchers and transformers
SELECT i, j, COLUMNS(i, j, k) APPLY(toFloat64), COLUMNS(i, j) EXCEPT (i) from columns_transformers;
EXPLAIN SYNTAX SELECT i, j, COLUMNS(i, j, k) APPLY(toFloat64), COLUMNS(i, j) EXCEPT (i) from columns_transformers;

-- APPLY with parameterized function
SELECT COLUMNS(i, j, k) APPLY(quantiles(0.5)) from columns_transformers;
EXPLAIN SYNTAX SELECT COLUMNS(i, j, k) APPLY(quantiles(0.5)) from columns_transformers;

DROP TABLE columns_transformers;
