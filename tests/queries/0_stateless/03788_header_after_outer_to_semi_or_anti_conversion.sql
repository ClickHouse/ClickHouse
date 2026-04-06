SET enable_analyzer = 1;

SELECT DISTINCT 2 FROM (SELECT 2 AS b, 1 AS a GROUP BY 1, toUInt256(1), 1, and(and(2, rand(materialize(toLowCardinality(2))), assumeNotNull(isNull(isNullable(2))) AND (and(rand(isZeroOrNull(2)), 2, *) AND 1) AND (assumeNotNull(2) AND rand(2)) AND (* AND isNotNull(toLowCardinality(2))), (* AND rand(isNullable(2))) AND isNotNull(2), isNullable(materialize(1)), *), 2, rand(toLowCardinality(2))) WITH CUBE WITH TOTALS) AS foo ANY LEFT JOIN (SELECT * AND ((isNullable(isZeroOrNull(toNullable(1))) AND *) AND rand(2)), 2 AS b, 1 AS a) AS bar ON and(foo.b = bar.b, foo.a = bar.b) WHERE (and(*, toNullable(2), * AND * AND isNotNull(2) AND * AND and(*) AND isNotNull(1), rand(2)) AND *) AND (1 AND assumeNotNull(2)) AND assumeNotNull(isNull(2)) AND (* AND (* AND isZeroOrNull(2)) AND isNull(toNullable(2))) ORDER BY ALL ASC NULLS FIRST;

SELECT DISTINCT 2
FROM
    (SELECT 2 AS b, 1 AS a GROUP BY 1, toUInt256(1), 1 WITH CUBE WITH TOTALS) AS foo
  ANY LEFT JOIN
    (SELECT 2 AS b, 1 AS a) AS bar
  ON and(foo.b = bar.b, foo.a = bar.b)
WHERE 0;
