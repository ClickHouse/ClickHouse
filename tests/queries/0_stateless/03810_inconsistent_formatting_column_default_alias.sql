-- Test that DEFAULT/MATERIALIZED/EPHEMERAL/ALIAS column expressions with aliases are formatted consistently
-- (wrapped in parentheses to avoid ambiguity)

-- DEFAULT with alias (parentheses required)
SELECT formatQuerySingleLine('CREATE TABLE t (c0 Int64 DEFAULT (1 AS a)) ENGINE = MergeTree ORDER BY c0');

-- Test roundtrip
SELECT formatQuerySingleLine(formatQuerySingleLine('CREATE TABLE t (c0 Int64 DEFAULT (1 AS a)) ENGINE = MergeTree ORDER BY c0'));

-- MATERIALIZED with alias
SELECT formatQuerySingleLine('CREATE TABLE t (c0 Int64, c1 Int64 MATERIALIZED (c0 + 1 AS a)) ENGINE = MergeTree ORDER BY c0');
SELECT formatQuerySingleLine(formatQuerySingleLine('CREATE TABLE t (c0 Int64, c1 Int64 MATERIALIZED (c0 + 1 AS a)) ENGINE = MergeTree ORDER BY c0'));

-- ALIAS with alias
SELECT formatQuerySingleLine('CREATE TABLE t (c0 Int64, c1 Int64 ALIAS (c0 * 2 AS doubled)) ENGINE = MergeTree ORDER BY c0');
SELECT formatQuerySingleLine(formatQuerySingleLine('CREATE TABLE t (c0 Int64, c1 Int64 ALIAS (c0 * 2 AS doubled)) ENGINE = MergeTree ORDER BY c0'));

-- DEFAULT with alias followed by another column
SELECT formatQuerySingleLine('CREATE TABLE t (c0 Int64 DEFAULT (1 AS a), c1 String) ENGINE = MergeTree ORDER BY c0');

-- More complex expression with alias in DEFAULT
SELECT formatQuerySingleLine('CREATE TABLE t (c0 Int64 DEFAULT (toInt64(now()) AS ts)) ENGINE = MergeTree ORDER BY c0');
