SET enable_analyzer = 1;
DROP TABLE IF EXISTS columns_transformers;

CREATE TABLE columns_transformers (i Int64, j Int16, k Int64) Engine=TinyLog;
INSERT INTO columns_transformers VALUES (100, 10, 324), (120, 8, 23);

SELECT * APPLY(sum) from columns_transformers;
SELECT * APPLY sum from columns_transformers;
SELECT columns_transformers.* APPLY(avg) from columns_transformers;
SELECT a.* APPLY(toDate) APPLY(any) from columns_transformers a;
SELECT COLUMNS('[jk]') APPLY(toString) APPLY(length) from columns_transformers;

SELECT * EXCEPT(i) APPLY(sum) from columns_transformers;
SELECT columns_transformers.* EXCEPT(j) APPLY(avg) from columns_transformers;
-- EXCEPT after APPLY will not match anything
SELECT a.* APPLY(toDate) EXCEPT(i, j) APPLY(any) from columns_transformers a;

SELECT * EXCEPT STRICT i from columns_transformers;
SELECT * EXCEPT STRICT (i, j) from columns_transformers;
SELECT * EXCEPT STRICT i, j1 from columns_transformers; -- { serverError UNKNOWN_IDENTIFIER }
SELECT * EXCEPT STRICT(i, j1) from columns_transformers; -- { serverError NO_SUCH_COLUMN_IN_TABLE , BAD_ARGUMENTS }
SELECT * REPLACE STRICT i + 1 AS i from columns_transformers;
SELECT * REPLACE STRICT(i + 1 AS col) from columns_transformers; -- { serverError NO_SUCH_COLUMN_IN_TABLE, BAD_ARGUMENTS }
SELECT * REPLACE(i + 1 AS i) APPLY(sum) from columns_transformers;
SELECT columns_transformers.* REPLACE(j + 2 AS j, i + 1 AS i) APPLY(avg) from columns_transformers;
SELECT columns_transformers.* REPLACE(j + 1 AS j, j + 2 AS j) APPLY(avg) from columns_transformers; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
-- REPLACE after APPLY will not match anything
SELECT a.* APPLY(toDate) REPLACE(i + 1 AS i) APPLY(any) from columns_transformers a;
SELECT a.* APPLY(toDate) REPLACE STRICT(i + 1 AS i) APPLY(any) from columns_transformers a; -- { serverError NO_SUCH_COLUMN_IN_TABLE, BAD_ARGUMENTS }

EXPLAIN SYNTAX SELECT * APPLY(sum) from columns_transformers;
EXPLAIN SYNTAX SELECT columns_transformers.* APPLY(avg) from columns_transformers;
EXPLAIN SYNTAX SELECT a.* APPLY(toDate) APPLY(any) from columns_transformers a;
EXPLAIN SYNTAX SELECT COLUMNS('[jk]') APPLY(toString) APPLY(length) from columns_transformers;
EXPLAIN SYNTAX SELECT * EXCEPT(i) APPLY(sum) from columns_transformers;
EXPLAIN SYNTAX SELECT columns_transformers.* EXCEPT(j) APPLY(avg) from columns_transformers;
EXPLAIN SYNTAX SELECT a.* APPLY(toDate) EXCEPT(i, j) APPLY(any) from columns_transformers a;
EXPLAIN SYNTAX SELECT * REPLACE(i + 1 AS i) APPLY(sum) from columns_transformers;
EXPLAIN AST SELECT * REPLACE(i + 1 AS i) APPLY(sum) from columns_transformers;
EXPLAIN SYNTAX SELECT sum(i + 1 AS m) from columns_transformers;
EXPLAIN AST SELECT sum(i + 1 AS m) from columns_transformers;
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

-- Coverage for MatcherNode.cpp and ColumnTransformers.cpp: qualified COLUMNS, regex EXCEPT,
-- and invalid-regex error paths not covered by existing tests.

CREATE TABLE t_matcher (a UInt64, b_x UInt64, b_y UInt64) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_matcher VALUES (1, 2, 3);

-- 1. Invalid COLUMNS regex — hits MatcherNode.cpp:106-108 (CANNOT_COMPILE_REGEXP throw)
SELECT COLUMNS('[') FROM numbers(1); -- { serverError CANNOT_COMPILE_REGEXP }

-- 2. Qualified COLUMNS regexp — hits MatcherNode.cpp:282-298 in toASTImpl
EXPLAIN SYNTAX SELECT t_matcher.COLUMNS('^b') FROM t_matcher;

-- 3. Qualified COLUMNS list — hits MatcherNode.cpp:325-342 in toASTImpl
EXPLAIN SYNTAX SELECT t_matcher.COLUMNS(b_x, b_y) FROM t_matcher;

-- 4. Qualified COLUMNS regexp + APPLY transformer — hits MatcherNode.cpp:291-295
EXPLAIN SYNTAX SELECT t_matcher.COLUMNS('^b') APPLY(toString) FROM t_matcher;

-- 5. Qualified COLUMNS list + APPLY transformer — hits MatcherNode.cpp:335-339
EXPLAIN SYNTAX SELECT t_matcher.COLUMNS(b_x, b_y) APPLY(toString) FROM t_matcher;

-- 6. EXCEPT with regex string literal — hits ColumnTransformers.cpp:233-237
EXPLAIN SYNTAX SELECT * EXCEPT ('^b') FROM t_matcher;

DROP TABLE t_matcher;
