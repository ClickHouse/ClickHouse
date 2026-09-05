-- Regression test: the signedness flag handed to compiled binary bodies was computed with
-- `std::is_signed_v`, which is `false` for `Int128` because `Int128` is a class type. The compiled
-- path then used unsigned instructions and disagreed with the interpreted path.
-- https://github.com/ClickHouse/ClickHouse/issues/117614

SELECT 'Int128 JIT off' AS tag,
    bitNot(toInt128(c0)) AS x,
    least(x, materialize(toInt128(0))) AS l,
    greatest(x, materialize(toInt128(0))) AS g,
    bitShiftRight(x, materialize(toUInt8(3))) AS s,
    midpoint(x, materialize(toInt128(-1))) AS m
FROM values('c0 UInt8', 7, 127, 230) ORDER BY c0
SETTINGS compile_expressions = 0, min_count_to_compile_expression = 0;

SELECT 'Int128 JIT on' AS tag,
    bitNot(toInt128(c0)) AS x,
    least(x, materialize(toInt128(0))) AS l,
    greatest(x, materialize(toInt128(0))) AS g,
    bitShiftRight(x, materialize(toUInt8(3))) AS s,
    midpoint(x, materialize(toInt128(-1))) AS m
FROM values('c0 UInt8', 7, 127, 230) ORDER BY c0
SETTINGS compile_expressions = 1, min_count_to_compile_expression = 0;

SELECT 'UInt128 JIT off' AS tag,
    bitNot(toUInt128(c0)) AS u,
    midpoint(u, materialize(toUInt128(1))) AS m
FROM values('c0 UInt8', 7, 127, 230) ORDER BY c0
SETTINGS compile_expressions = 0, min_count_to_compile_expression = 0;

SELECT 'UInt128 JIT on' AS tag,
    bitNot(toUInt128(c0)) AS u,
    midpoint(u, materialize(toUInt128(1))) AS m
FROM values('c0 UInt8', 7, 127, 230) ORDER BY c0
SETTINGS compile_expressions = 1, min_count_to_compile_expression = 0;
