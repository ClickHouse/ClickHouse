-- Regression: segfault in executeActionForPartialResult when filter expression contains arrayJoin
-- and the convertOuterJoinToInnerJoin optimization tries to evaluate the filter with partial (null) arguments.

SET enable_analyzer = 1;
SELECT DISTINCT
    2,
    1048575
FROM numbers(1) AS l,
    numbers(2, isZeroOrNull(assumeNotNull(1))) AS r
ANY INNER JOIN r AS alias37 ON equals(alias37.number, r.number)
RIGHT JOIN l AS alias44 ON equals(alias44.number, alias37.number)
ANY INNER JOIN alias44 AS alias48 ON equals(alias48.number, r.number)
ANY RIGHT JOIN r AS alias52 ON equals(alias52.number, alias37.number)
WHERE equals(isNull(toLowCardinality(toUInt128(2))), arrayJoin([*, 13, 13, 13, toNullable(13), 13]))
GROUP BY
    materialize(1),
    isNull(toUInt128(2)),
    and(and(1048575, isZeroOrNull(1), isNullable(isNull(1))), materialize(13), isNull(toUInt256(materialize(2))), *, and(*, and(1, nan, isNull(isNull(1)), isZeroOrNull(1), 1048575), 13))
WITH CUBE;
