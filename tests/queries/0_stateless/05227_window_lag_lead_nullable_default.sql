-- { echo }

-- A NULL default widens the result type and keeps values from the input column.
SELECT
    toTypeName(lag(number, 1, NULL) OVER (ORDER BY number)),
    toTypeName(lead(number, 1, NULL) OVER (ORDER BY number)),
    toTypeName(lagInFrame(number, 1, NULL) OVER (ORDER BY number ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)),
    toTypeName(leadInFrame(number, 1, NULL) OVER (ORDER BY number ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)),
    lag(number, 1, NULL) OVER (ORDER BY number),
    lead(number, 1, NULL) OVER (ORDER BY number),
    lagInFrame(number, 1, NULL) OVER (ORDER BY number ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING),
    leadInFrame(number, 1, NULL) OVER (ORDER BY number ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
FROM numbers(2);

-- A typed NULL default is supported as well.
SELECT
    toTypeName(leadInFrame(number, 1, CAST(NULL, 'Nullable(UInt64)')) OVER (ORDER BY number ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)),
    leadInFrame(number, 1, CAST(NULL, 'Nullable(UInt64)')) OVER (ORDER BY number ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
FROM numbers(1);

-- A non-null default can widen the result type too.
SELECT
    toTypeName(leadInFrame(toUInt8(number), 1, toInt16(-1)) OVER (ORDER BY number ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)),
    leadInFrame(toUInt8(number), 1, toInt16(-1)) OVER (ORDER BY number ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
FROM numbers(2);