-- Test that CONSTRAINT CHECK expressions with aliases are formatted consistently
-- (wrapped in parentheses to avoid ambiguity)

-- CONSTRAINT with alias in CHECK expression (parentheses required)
SELECT formatQuerySingleLine('CREATE TABLE t (c0 Int64, CONSTRAINT c CHECK (c0 AS a)) ENGINE = MergeTree ORDER BY c0');

-- Test roundtrip
SELECT formatQuerySingleLine(formatQuerySingleLine('CREATE TABLE t (c0 Int64, CONSTRAINT c CHECK (c0 AS a)) ENGINE = MergeTree ORDER BY c0'));

-- CONSTRAINT with alias followed by another constraint
SELECT formatQuerySingleLine('CREATE TABLE t (c0 Int64, CONSTRAINT c0 CHECK (c0 AS a), CONSTRAINT c1 CHECK (c0 > 0)) ENGINE = MergeTree ORDER BY c0');

-- CONSTRAINT with alias followed by column
SELECT formatQuerySingleLine('CREATE TABLE t (c0 Int64, CONSTRAINT c CHECK (c0 AS a), c1 String) ENGINE = MergeTree ORDER BY c0');

-- More complex expression with alias in CONSTRAINT
SELECT formatQuerySingleLine('CREATE TABLE t (c0 Int64, CONSTRAINT c CHECK (c0 + 1 AS a)) ENGINE = MergeTree ORDER BY c0');

-- ASSUME constraint with alias
SELECT formatQuerySingleLine('CREATE TABLE t (c0 Int64, CONSTRAINT c ASSUME (c0 > 0 AS positive)) ENGINE = MergeTree ORDER BY c0');
SELECT formatQuerySingleLine(formatQuerySingleLine('CREATE TABLE t (c0 Int64, CONSTRAINT c ASSUME (c0 > 0 AS positive)) ENGINE = MergeTree ORDER BY c0'));
