-- Test that INDEX and TTL expressions with aliases are formatted consistently
-- (wrapped in parentheses to avoid ambiguity)
-- Note: aliases in INDEX and TTL require parentheses in input

-- INDEX with alias (parentheses required)
SELECT formatQuerySingleLine('CREATE TABLE t (c0 Int64, INDEX i0 (c0 AS a) TYPE minmax) ENGINE = MergeTree ORDER BY c0');

-- TTL with alias (table level, parentheses required)
SELECT formatQuerySingleLine('CREATE TABLE t (c0 Date) ENGINE = MergeTree ORDER BY () TTL (c0 AS a)');

-- TTL with alias (column level)
SELECT formatQuerySingleLine('CREATE TABLE t (c0 Date TTL (c0 AS a)) ENGINE = MergeTree ORDER BY ()');

-- Test that the formatted query can be parsed again (roundtrip)
SELECT formatQuerySingleLine(formatQuerySingleLine('CREATE TABLE t (c0 Int64, INDEX i0 (c0 AS a) TYPE minmax) ENGINE = MergeTree ORDER BY c0'));
SELECT formatQuerySingleLine(formatQuerySingleLine('CREATE TABLE t (c0 Date) ENGINE = MergeTree ORDER BY () TTL (c0 AS a)'));

-- CREATE TABLE ... AS SELECT should still work correctly (not confused with alias)
SELECT formatQuerySingleLine('CREATE TABLE t ENGINE = Memory AS SELECT 1');
SELECT formatQuerySingleLine('CREATE TABLE t (c0 Int64) ENGINE = Memory AS SELECT 1');

-- More complex expressions with aliases in INDEX
SELECT formatQuerySingleLine('CREATE TABLE t (c0 String, INDEX i0 (lower(c0) AS lc) TYPE tokenbf_v1(1024, 2, 0)) ENGINE = MergeTree ORDER BY c0');

-- Test ambiguous cases where alias is followed by table keywords
-- TTL with alias followed by SETTINGS
SELECT formatQuerySingleLine('CREATE TABLE t (c0 Date) ENGINE = MergeTree ORDER BY () TTL (c0 AS a) SETTINGS index_granularity = 8192');
SELECT formatQuerySingleLine(formatQuerySingleLine('CREATE TABLE t (c0 Date) ENGINE = MergeTree ORDER BY () TTL (c0 AS a) SETTINGS index_granularity = 8192'));

-- TTL with alias followed by PARTITION BY
SELECT formatQuerySingleLine('CREATE TABLE t (c0 Date) ENGINE = MergeTree ORDER BY () TTL (c0 AS a) PARTITION BY c0');
SELECT formatQuerySingleLine(formatQuerySingleLine('CREATE TABLE t (c0 Date) ENGINE = MergeTree ORDER BY () TTL (c0 AS a) PARTITION BY c0'));
