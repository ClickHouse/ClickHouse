DROP TABLE IF EXISTS simple_agf_any_summing_mt;

CREATE TABLE simple_agf_any_summing_mt
(
    a Int64,
    any_simple SimpleAggregateFunction(any_respect_nulls, Nullable(UInt64)),
    any_agg AggregateFunction(any_respect_nulls, Nullable(UInt64)),
    anyLast_simple SimpleAggregateFunction(anyLast_respect_nulls, Nullable(UInt64)),
    anyLast_agg AggregateFunction(anyLast_respect_nulls, Nullable(UInt64))
)
ENGINE = SummingMergeTree
ORDER BY a;

INSERT INTO simple_agf_any_summing_mt SELECT
    a,
    any_respect_nulls(any_simple),
    any_respect_nullsState(any_agg),
    anyLast_respect_nulls(anyLast_simple),
    anyLast_respect_nullsState(anyLast_agg)
FROM
(
    SELECT
        42 AS a,
        NULL::Nullable(UInt64) AS any_simple,
        NULL::Nullable(UInt64) AS any_agg,
        NULL::Nullable(UInt64) AS anyLast_simple,
        NULL::Nullable(UInt64) AS anyLast_agg
)
GROUP BY a;

INSERT INTO simple_agf_any_summing_mt SELECT
    number % 51 as a,
    any_respect_nulls(toNullable(number)),
    any_respect_nullsState(toNullable(number)),
    anyLast_respect_nulls(toNullable(number)),
    anyLast_respect_nullsState(toNullable(number)),
FROM numbers(10000)
GROUP BY a;

INSERT INTO simple_agf_any_summing_mt SELECT
    a,
    any_respect_nulls(any_simple),
    any_respect_nullsState(any_agg),
    anyLast_respect_nulls(anyLast_simple),
    anyLast_respect_nullsState(anyLast_agg)
FROM
(
    SELECT
        50 AS a,
        NULL::Nullable(UInt64) AS any_simple,
        NULL::Nullable(UInt64) AS any_agg,
        NULL::Nullable(UInt64) AS anyLast_simple,
        NULL::Nullable(UInt64) AS anyLast_agg
)
GROUP BY a;

OPTIMIZE TABLE simple_agf_any_summing_mt FINAL;

SELECT
    a,
    any_respect_nulls(any_simple),
    any_respect_nullsMerge(any_agg),
    anyLast_respect_nulls(anyLast_simple),
    anyLast_respect_nullsMerge(anyLast_agg)
FROM simple_agf_any_summing_mt
GROUP BY a
ORDER BY a;
