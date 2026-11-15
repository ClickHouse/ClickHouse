-- Regression test: JIT miscompiled sign(Int64) by comparing Int64 argument against Int8 zero constant.
-- Values outside the Int8 range (-128..127) would get the wrong sign.
-- https://github.com/ClickHouse/ClickHouse/issues/97997

SELECT sign(x) * (toFloat64(2) - toFloat64(1))
FROM values('x Int64', -3600000, -129, -128, -1, 0, 1, 127, 128, 129, 3600000)
ORDER BY x ASC
SETTINGS compile_expressions = 1, min_count_to_compile_expression = 0;
