-- Expressions containing subqueries must not be shared between use sites when
-- extracted from the identifier resolve cache: later stages (GLOBAL IN rewrite,
-- rewrite_in_to_join, createUniqueAliasesIfNecessary) mutate each instance
-- independently, so every retrieval must return its own clone.

SET enable_analyzer = 1;
SET enable_identifier_resolve_cache = 1;

-- Note: the GLOBAL IN + parallel replicas case is deliberately NOT duplicated here;
-- it is already covered by 03316_analyzer_unique_table_aliases_dist.

SELECT '-- IN subquery alias reused (3 references)';
SELECT (number IN (SELECT number AS n FROM numbers(3))) AS cond, cond AS c2, NOT cond AS c3
FROM numbers(5) ORDER BY number;

SELECT '-- inner alias must be stripped from every embedded copy (remote re-analysis)';
SELECT (number IN (SELECT number AS inner_a FROM numbers(3))) AS c1, c1 AS c2, c1 AS c3
FROM remote('localhost', numbers(5)) ORDER BY 1;

SELECT '-- exists alias reused';
SELECT exists((SELECT 1)) AS e, e AS e2 FROM numbers(2);

SELECT '-- scalar subquery alias reused';
WITH (SELECT max(number) FROM numbers(10)) AS m SELECT m + 1 AS a, m + 2 AS b FROM numbers(1);

SELECT '-- IN subquery alias reused with rewrite_in_to_join';
SELECT (number IN (SELECT number FROM numbers(3))) AS cond, cond AS c2
FROM numbers(5) ORDER BY number
SETTINGS rewrite_in_to_join = 1;

SELECT '-- group_by_use_nulls: nullable key outside aggregate, original type inside';
SELECT number + 1 AS a, sum(a), count()
FROM numbers(4)
GROUP BY ROLLUP(a)
ORDER BY a
SETTINGS group_by_use_nulls = 1;
