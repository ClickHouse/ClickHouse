-- `Decimal` to integer and to float conversions must agree with the interpreter when they are
-- JIT-compiled, at a non-zero scale and for negative values. `+ 0` is what makes each conversion
-- eligible for compilation: alone it has no compilable neighbour and stays interpreted. The
-- `Decimal128` divider is built out of the limbs of `10^scale`, and its high limb is zero below
-- scale 20, so only a scale of 20 or more discriminates a limb-order or a dropped-limb bug.
SET compile_expressions = 1;
SET min_count_to_compile_expression = 0;

SELECT toInt32(materialize(-5.1234::Decimal32(4))) + 0,
       toInt64(materialize(-123.45678901::Decimal64(8))) + 0,
       toInt64(materialize(-0.5::Decimal64(1))) + 0,
       toInt128(materialize(-999.12345678901234567890::Decimal128(20))) + 0,
       toFloat64(materialize(-123.45::Decimal64(2))) + 0,
       CAST(materialize(-42.5::Decimal64(1)) AS Int64) + 0;

SELECT toInt32(materialize(-5.1234::Decimal32(4))) + 0,
       toInt64(materialize(-123.45678901::Decimal64(8))) + 0,
       toInt64(materialize(-0.5::Decimal64(1))) + 0,
       toInt128(materialize(-999.12345678901234567890::Decimal128(20))) + 0,
       toFloat64(materialize(-123.45::Decimal64(2))) + 0,
       CAST(materialize(-42.5::Decimal64(1)) AS Int64) + 0
SETTINGS compile_expressions = 0;

-- Both rows above are value oracles over conversions that agree in either path, so neither reddens
-- if these shapes stop being compiled. The shapes below pin which of them compiles: one per divider
-- arm, the `CAST` gate separately, and the same conversion stripped of its `+ 0`, which has nothing
-- left to compile.
SELECT toInt64(materialize(-123.45678901::Decimal64(8))) + 0 FROM numbers(2)
    SETTINGS compile_expressions = 1, min_count_to_compile_expression = 0, log_comment = '05255_int' FORMAT Null;
SELECT toInt128(materialize(-999.12345678901234567890::Decimal128(20))) + 0 FROM numbers(2)
    SETTINGS compile_expressions = 1, min_count_to_compile_expression = 0, log_comment = '05255_limbs' FORMAT Null;
SELECT toFloat64(materialize(-123.45::Decimal64(2))) + 0 FROM numbers(2)
    SETTINGS compile_expressions = 1, min_count_to_compile_expression = 0, log_comment = '05255_float' FORMAT Null;
SELECT CAST(materialize(-42.5::Decimal64(1)) AS Int64) + 0 FROM numbers(2)
    SETTINGS compile_expressions = 1, min_count_to_compile_expression = 0, log_comment = '05255_cast' FORMAT Null;
SELECT toInt64(materialize(-123.45678901::Decimal64(8))) FROM numbers(2)
    SETTINGS compile_expressions = 1, min_count_to_compile_expression = 0, log_comment = '05255_alone' FORMAT Null;
SELECT materialize(2.0) + materialize(0.0) + materialize(1.0) FROM numbers(2)
    SETTINGS compile_expressions = 1, min_count_to_compile_expression = 0, log_comment = '05255_control' FORMAT Null;

SYSTEM FLUSH LOGS query_log;

WITH shapes AS
(
    SELECT log_comment, argMax(ProfileEvents['CompiledFunctionExecute'] > 0, event_time_microseconds) AS compiled
    FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment LIKE '05255_%'
    GROUP BY log_comment
)
-- The control keeps the first four columns honest in a build without the embedded compiler, where
-- every shape is interpreted and an absolute assertion would go green on nothing being compiled.
SELECT
    (SELECT compiled FROM shapes WHERE log_comment = '05255_int')
        = (SELECT compiled FROM shapes WHERE log_comment = '05255_control'),
    (SELECT compiled FROM shapes WHERE log_comment = '05255_limbs')
        = (SELECT compiled FROM shapes WHERE log_comment = '05255_control'),
    (SELECT compiled FROM shapes WHERE log_comment = '05255_float')
        = (SELECT compiled FROM shapes WHERE log_comment = '05255_control'),
    (SELECT compiled FROM shapes WHERE log_comment = '05255_cast')
        = (SELECT compiled FROM shapes WHERE log_comment = '05255_control'),
    (SELECT compiled FROM shapes WHERE log_comment = '05255_alone') = 0;
