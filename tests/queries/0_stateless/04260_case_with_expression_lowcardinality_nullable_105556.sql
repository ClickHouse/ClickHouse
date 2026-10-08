-- https://github.com/ClickHouse/ClickHouse/pull/105556#discussion_r3282578123
-- Ensure CASE NULL = NULL semantics also hold when WHEN supertype is LowCardinality(Nullable).

SET allow_suspicious_low_cardinality_types = 1;

SELECT caseWithExpression(materialize(CAST(NULL AS LowCardinality(Nullable(Int32)))), CAST(NULL AS LowCardinality(Nullable(Int32))), 'YES', 'NO');
SELECT caseWithExpression(materialize(CAST(NULL AS LowCardinality(Nullable(String)))), CAST(NULL AS LowCardinality(Nullable(String))), 'YES', 'NO');

DROP TABLE IF EXISTS t_case_lc_nullable;
CREATE TABLE t_case_lc_nullable (x LowCardinality(Nullable(Int32))) ENGINE = Memory;
INSERT INTO t_case_lc_nullable VALUES (NULL), (1), (2);

SELECT
    x,
    caseWithExpression(x,
        CAST(NULL AS LowCardinality(Nullable(Int32))), 'null_match',
        CAST(1 AS LowCardinality(Nullable(Int32))), 'one',
        'else')
FROM t_case_lc_nullable
ORDER BY x NULLS FIRST;

DROP TABLE t_case_lc_nullable;
