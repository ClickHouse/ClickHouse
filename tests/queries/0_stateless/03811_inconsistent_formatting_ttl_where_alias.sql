-- Test that TTL WHERE expressions with aliases are formatted consistently
-- (wrapped in parentheses to avoid ambiguity)

-- ALTER TABLE with TTL WHERE with alias
SELECT formatQuerySingleLine('ALTER TABLE t (MODIFY TTL c0 DELETE WHERE (0.761 AS a0))');

-- Test round-trip parsing
SELECT formatQuerySingleLine(formatQuerySingleLine('ALTER TABLE t (MODIFY TTL c0 DELETE WHERE (0.761 AS a0))'));
