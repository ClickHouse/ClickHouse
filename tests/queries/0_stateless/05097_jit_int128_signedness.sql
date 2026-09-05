-- Regression test: the signedness flag handed to compiled binary bodies was computed with
-- `std::is_signed_v`, which is `false` for `Int128` because `Int128` is a class type. The compiled
-- path then used unsigned instructions and disagreed with the interpreted path.
-- https://github.com/ClickHouse/ClickHouse/issues/117614

SELECT 'Int128 JIT off' AS tag,
    bitNot(toInt128(c0)) AS x,
    least(x, materialize(toInt128(0))) AS l,
    greatest(x, materialize(toInt128(0))) AS g,
    bitShiftRight(x, materialize(toUInt8(3))) AS s,
    midpoint(x, materialize(toInt128(-1))) AS m,
    midpoint(x, materialize(toInt128(255))) AS mp
FROM values('c0 UInt8', 7, 127, 230) ORDER BY c0
SETTINGS compile_expressions = 0, min_count_to_compile_expression = 0;

SELECT 'Int128 JIT on' AS tag,
    bitNot(toInt128(c0)) AS x,
    least(x, materialize(toInt128(0))) AS l,
    greatest(x, materialize(toInt128(0))) AS g,
    bitShiftRight(x, materialize(toUInt8(3))) AS s,
    midpoint(x, materialize(toInt128(-1))) AS m,
    midpoint(x, materialize(toInt128(255))) AS mp
FROM values('c0 UInt8', 7, 127, 230) ORDER BY c0
SETTINGS compile_expressions = 1, min_count_to_compile_expression = 0,
         log_comment = '05097_int128';

SELECT 'UInt128 JIT off' AS tag,
    bitNot(toUInt128(c0)) AS u,
    midpoint(u, materialize(toUInt128(1))) AS m
FROM values('c0 UInt8', 7, 127, 230) ORDER BY c0
SETTINGS compile_expressions = 0, min_count_to_compile_expression = 0;

SELECT 'UInt128 JIT on' AS tag,
    bitNot(toUInt128(c0)) AS u,
    midpoint(u, materialize(toUInt128(1))) AS m
FROM values('c0 UInt8', 7, 127, 230) ORDER BY c0
SETTINGS compile_expressions = 1, min_count_to_compile_expression = 0,
         log_comment = '05097_uint128';

-- Control: a shape this fix does not change the compilability of.
SELECT least(bitNot(toInt64(c0)), materialize(toInt64(0)))
FROM values('c0 UInt8', 7, 127, 230)
SETTINGS compile_expressions = 1, min_count_to_compile_expression = 0,
         log_comment = '05097_control' FORMAT Null;

SYSTEM FLUSH LOGS query_log;

-- The comparisons above would all pass if nothing were compiled. This row pins that the compiled
-- path is the one under test, and comparing the shapes with a control instead of pinning a literal
-- keeps it green in a build with no embedded compiler, where all three are 0.
WITH shapes AS
(
    SELECT log_comment, argMax(ProfileEvents['CompiledFunctionExecute'] > 0, event_time_microseconds) AS compiled
    FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish'
      AND log_comment IN ('05097_int128', '05097_uint128', '05097_control')
    GROUP BY log_comment
)
SELECT count() = 3 AND uniqExact(compiled) = 1 FROM shapes;
