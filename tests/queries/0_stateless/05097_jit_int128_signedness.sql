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
    midpoint(x, materialize(toInt128(255))) AS mp,
    avg2(x, materialize(toInt128(-1))) AS a2
FROM values('c0 UInt8', 7, 127, 230) ORDER BY c0
SETTINGS compile_expressions = 0, min_count_to_compile_expression = 0;

SELECT 'Int128 JIT on' AS tag,
    bitNot(toInt128(c0)) AS x,
    least(x, materialize(toInt128(0))) AS l,
    greatest(x, materialize(toInt128(0))) AS g,
    bitShiftRight(x, materialize(toUInt8(3))) AS s,
    midpoint(x, materialize(toInt128(-1))) AS m,
    midpoint(x, materialize(toInt128(255))) AS mp,
    avg2(x, materialize(toInt128(-1))) AS a2
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

-- Every operand below is a plain column, so `bitNot` has no compilable child and is never compiled
-- on its own: the carrier is the only node that can raise CompiledFunctionExecute. The control must
-- be a compilable function that shares no `Impl` with any carrier: comparing against it keeps the
-- row green where nothing compiles, and still reddens when only the carriers stop being compiled.
SELECT least(bitNot(a), b)
FROM values('a Int128, b Int128, sh UInt8, d Int64, e Int64', (7, 0, 3, 7, 0), (127, 0, 3, 127, 0), (230, 0, 3, 230, 0))
SETTINGS compile_expressions = 1, min_count_to_compile_expression = 0, log_comment = '05097_live_least' FORMAT Null;

SELECT greatest(bitNot(a), b)
FROM values('a Int128, b Int128, sh UInt8, d Int64, e Int64', (7, 0, 3, 7, 0), (127, 0, 3, 127, 0), (230, 0, 3, 230, 0))
SETTINGS compile_expressions = 1, min_count_to_compile_expression = 0, log_comment = '05097_live_greatest' FORMAT Null;

SELECT bitShiftRight(bitNot(a), sh)
FROM values('a Int128, b Int128, sh UInt8, d Int64, e Int64', (7, 0, 3, 7, 0), (127, 0, 3, 127, 0), (230, 0, 3, 230, 0))
SETTINGS compile_expressions = 1, min_count_to_compile_expression = 0, log_comment = '05097_live_shift' FORMAT Null;

SELECT midpoint(bitNot(a), b)
FROM values('a Int128, b Int128, sh UInt8, d Int64, e Int64', (7, 0, 3, 7, 0), (127, 0, 3, 127, 0), (230, 0, 3, 230, 0))
SETTINGS compile_expressions = 1, min_count_to_compile_expression = 0, log_comment = '05097_live_midpoint' FORMAT Null;

SELECT avg2(bitNot(a), b)
FROM values('a Int128, b Int128, sh UInt8, d Int64, e Int64', (7, 0, 3, 7, 0), (127, 0, 3, 127, 0), (230, 0, 3, 230, 0))
SETTINGS compile_expressions = 1, min_count_to_compile_expression = 0, log_comment = '05097_live_avg2' FORMAT Null;

SELECT plus(bitNot(d), e)
FROM values('a Int128, b Int128, sh UInt8, d Int64, e Int64', (7, 0, 3, 7, 0), (127, 0, 3, 127, 0), (230, 0, 3, 230, 0))
SETTINGS compile_expressions = 1, min_count_to_compile_expression = 0, log_comment = '05097_live_control' FORMAT Null;

SYSTEM FLUSH LOGS query_log;

WITH shapes AS
(
    SELECT log_comment, argMax(ProfileEvents['CompiledFunctionExecute'] > 0, event_time_microseconds) AS compiled
    FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish'
      AND log_comment LIKE '05097_live_%'
    GROUP BY log_comment
)
SELECT
    (SELECT compiled FROM shapes WHERE log_comment = '05097_live_least')
        = (SELECT compiled FROM shapes WHERE log_comment = '05097_live_control'),
    (SELECT compiled FROM shapes WHERE log_comment = '05097_live_greatest')
        = (SELECT compiled FROM shapes WHERE log_comment = '05097_live_control'),
    (SELECT compiled FROM shapes WHERE log_comment = '05097_live_shift')
        = (SELECT compiled FROM shapes WHERE log_comment = '05097_live_control'),
    (SELECT compiled FROM shapes WHERE log_comment = '05097_live_midpoint')
        = (SELECT compiled FROM shapes WHERE log_comment = '05097_live_control'),
    (SELECT compiled FROM shapes WHERE log_comment = '05097_live_avg2')
        = (SELECT compiled FROM shapes WHERE log_comment = '05097_live_control');
