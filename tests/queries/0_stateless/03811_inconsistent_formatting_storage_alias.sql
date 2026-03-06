-- Test that storage clause expressions with aliases are formatted consistently
-- (wrapped in parentheses to avoid ambiguity)

-- PRIMARY KEY with alias
SELECT formatQuerySingleLine('CREATE TABLE t (c0 Int64) ENGINE = MergeTree PRIMARY KEY (c0 AS a) ORDER BY c0');

-- Test roundtrip for PRIMARY KEY
SELECT formatQuerySingleLine(formatQuerySingleLine('CREATE TABLE t (c0 Int64) ENGINE = MergeTree PRIMARY KEY (c0 AS a) ORDER BY c0'));

-- PARTITION BY with alias
SELECT formatQuerySingleLine('CREATE TABLE t (c0 Int64) ENGINE = MergeTree PARTITION BY (c0 AS a) ORDER BY c0');

-- Test roundtrip for PARTITION BY
SELECT formatQuerySingleLine(formatQuerySingleLine('CREATE TABLE t (c0 Int64) ENGINE = MergeTree PARTITION BY (c0 AS a) ORDER BY c0'));

-- SAMPLE BY with alias
SELECT formatQuerySingleLine('CREATE TABLE t (c0 Int64) ENGINE = MergeTree ORDER BY c0 SAMPLE BY (c0 AS a)');

-- Test roundtrip for SAMPLE BY
SELECT formatQuerySingleLine(formatQuerySingleLine('CREATE TABLE t (c0 Int64) ENGINE = MergeTree ORDER BY c0 SAMPLE BY (c0 AS a)'));

-- Combined: multiple clauses with aliases
SELECT formatQuerySingleLine('CREATE TABLE t (c0 Int64) ENGINE = MergeTree PARTITION BY (c0 AS p) PRIMARY KEY (c0 AS k) ORDER BY c0 SAMPLE BY (c0 AS s)');

-- Test roundtrip for combined
SELECT formatQuerySingleLine(formatQuerySingleLine('CREATE TABLE t (c0 Int64) ENGINE = MergeTree PARTITION BY (c0 AS p) PRIMARY KEY (c0 AS k) ORDER BY c0 SAMPLE BY (c0 AS s)'));

-- Complex expression with alias in PRIMARY KEY
SELECT formatQuerySingleLine('CREATE TABLE t (c0 Int64) ENGINE = MergeTree PRIMARY KEY (c0 + 1 AS a) ORDER BY c0');

-- Test roundtrip for complex expression
SELECT formatQuerySingleLine(formatQuerySingleLine('CREATE TABLE t (c0 Int64) ENGINE = MergeTree PRIMARY KEY (c0 + 1 AS a) ORDER BY c0'));
