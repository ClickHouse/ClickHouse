-- The state type of a -Sparkbar combinator over a DateTime64 x-axis must round-trip through its
-- name: begin_x/end_x are DecimalField parameters and need ::Type suffixes to reparse.

SELECT toTypeName(countSparkbarState(3, toDateTime64('2024-01-01 00:00:00', 3), toDateTime64('2024-01-03 00:00:00', 3))(toDateTime64('2024-01-01 00:00:00', 3) + INTERVAL number DAY)) FROM numbers(3);

-- CAST to the printed state type name must reparse it.
SELECT countSparkbarMerge(3, toDateTime64('2024-01-01 00:00:00', 3), toDateTime64('2024-01-03 00:00:00', 3))(
    CAST(s, 'AggregateFunction(countSparkbar(3, \'1704067200\'::Decimal64(3), \'1704240000\'::Decimal64(3)), DateTime64(3))'))
FROM
(
    SELECT countSparkbarState(3, toDateTime64('2024-01-01 00:00:00', 3), toDateTime64('2024-01-03 00:00:00', 3))(toDateTime64('2024-01-01 00:00:00', 3) + INTERVAL intDiv(number, 2) DAY) AS s
    FROM numbers(5)
);

DROP TABLE IF EXISTS t_sparkbar_dt64_state;
CREATE TABLE t_sparkbar_dt64_state
(
    s AggregateFunction(countSparkbar(3, '1704067200'::Decimal64(3), '1704240000'::Decimal64(3)), DateTime64(3))
)
ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_sparkbar_dt64_state SELECT countSparkbarState(3, toDateTime64('2024-01-01 00:00:00', 3), toDateTime64('2024-01-03 00:00:00', 3))(toDateTime64('2024-01-01 00:00:00', 3) + INTERVAL number DAY) FROM numbers(3);
SELECT toTypeName(s) FROM t_sparkbar_dt64_state;
SELECT countSparkbarMerge(3, toDateTime64('2024-01-01 00:00:00', 3), toDateTime64('2024-01-03 00:00:00', 3))(s) FROM t_sparkbar_dt64_state;
DROP TABLE t_sparkbar_dt64_state;

-- Integer bounds keep the untyped spelling.
SELECT toTypeName(countSparkbarState(3, 0, 2)(number)) FROM numbers(3);
