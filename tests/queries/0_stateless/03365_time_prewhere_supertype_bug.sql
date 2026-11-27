SET enable_analyzer=1;

SELECT *
PREWHERE * OR ((8 OR * OR 1) OR 1 OR (toTime64(isZeroOrNull(13), 2) <= *) OR materialize(2))
GROUP BY
    toLowCardinality(materialize(2)),
    1,
    toTime64(isNullable(materialize(materialize(13))), * IS NULL) <= *; -- { serverError ILLEGAL_PREWHERE }
