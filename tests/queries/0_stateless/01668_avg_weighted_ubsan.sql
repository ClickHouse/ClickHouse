-- https://github.com/ClickHouse/ClickHouse/pull/19475
-- The result of this test is not important, but just so you know, it's wrong as it might overflow depending on which
-- underlying type is used. The expected result should be 10:
-- https://www.wolframalpha.com/input?i=%281023+*+1000000000.0+%2B+10+*+-9223372036854775808.0%29+%2F+%281000000000.0+%2B+-9223372036854775808.0%29
SELECT round(avgWeighted(x, y)) FROM (SELECT 1023 AS x, 1000000000 AS y UNION ALL SELECT 10 AS x, -9223372036854775808 AS y);
