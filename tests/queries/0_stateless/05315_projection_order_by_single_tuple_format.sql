-- Formatting must preserve a one-argument tuple used as a projection sorting expression.
SELECT position(formatQuerySingleLine('ALTER TABLE t ADD PROJECTION p (SELECT x ORDER BY tuple(''10000-01-01''))'), 'ORDER BY tuple(') > 0;
SELECT position(formatQuerySingleLine('ALTER TABLE t ADD PROJECTION p (SELECT x ORDER BY tuple(tuple(1)))'), 'ORDER BY tuple(tuple(1))') > 0;

-- Comma-separated sorting keys still use their canonical list formatting.
SELECT position(formatQuerySingleLine('ALTER TABLE t ADD PROJECTION p (SELECT x ORDER BY a, b)'), 'ORDER BY a, b') > 0;
