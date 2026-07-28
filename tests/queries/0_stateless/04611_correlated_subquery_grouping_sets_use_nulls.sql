-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/102273
-- A correlated scalar subquery referencing a `GROUPING SETS` key under
-- `group_by_use_nulls = 1` used to fail with a logical error:
--   "Unexpected return type from bitNot. Expected UInt64. Got Nullable(UInt64)"
-- because the correlated column kept its pre-Nullable type while the
-- post-aggregation rows carried Nullable values. Fixed in the analyzer
-- by https://github.com/ClickHouse/ClickHouse/pull/100365; `WITH ROLLUP` and
-- `WITH CUBE` shapes are covered by 04053_correlated_subquery_group_by_use_nulls,
-- this test pins the `GROUPING SETS` shape found by the fuzzer.

SET enable_analyzer = 1;
SET group_by_use_nulls = 1;

-- The row of the `()` set carries `number = NULL`: the correlated subquery
-- must be planned with the Nullable type and yield NULL for that row.
SELECT number, count() AS c, bitNot((SELECT bitNot(number))) AS x
FROM numbers(3)
GROUP BY GROUPING SETS ((number), ())
ORDER BY ALL ASC NULLS FIRST;

-- Same shape without `group_by_use_nulls`: the row of the `()` set carries
-- the default value instead of NULL.
SELECT number, count() AS c, bitNot((SELECT bitNot(number))) AS x
FROM numbers(3)
GROUP BY GROUPING SETS ((number), ())
ORDER BY ALL ASC NULLS FIRST
SETTINGS group_by_use_nulls = 0;

-- The original fuzzer query, scaled down from numbers(1023) to numbers(3).
SELECT x FROM
(
    SELECT intDiv(bitNot((SELECT bitNot(number))), 65536) AS x
    FROM numbers(3)
    GROUP BY GROUPING SETS ((bitNot(bitNot(number))), (), (), (*, '\0', bitNot(bitCount(number)) - (SELECT NULL LIMIT 1048577), toLowCardinality(toNullable('10.000100'))))
)
ORDER BY x ASC NULLS FIRST;
