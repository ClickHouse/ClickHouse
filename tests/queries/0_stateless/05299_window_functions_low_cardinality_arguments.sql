-- Window functions take LowCardinality arguments and return the plain type, both for copied values and defaults.
SELECT
    number,
    lag(toLowCardinality(toString(number))) OVER w,
    lead(toLowCardinality(toString(number)), 1, toLowCardinality('none')) OVER w,
    lagInFrame(toLowCardinality(number), 2, toLowCardinality(toUInt64(42))) OVER f,
    leadInFrame(toLowCardinality(toString(number))) OVER f,
    nth_value(toLowCardinality(toString(number)), 2) OVER f,
    first_value(toLowCardinality(toString(number))) OVER f,
    last_value(toLowCardinality(number)) OVER f,
    any(toLowCardinality(toString(number))) OVER f
FROM numbers(4)
WINDOW w AS (ORDER BY number), f AS (ORDER BY number ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING)
ORDER BY number;

SELECT
    toTypeName(lag(toLowCardinality(toString(number))) OVER ()),
    toTypeName(nth_value(toLowCardinality(number), 1) OVER ()),
    toTypeName(first_value(toLowCardinality(toString(number))) OVER ())
FROM numbers(1);
