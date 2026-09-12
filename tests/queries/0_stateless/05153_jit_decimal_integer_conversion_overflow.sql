-- https://github.com/ClickHouse/ClickHouse/issues/118958
SET compile_expressions = 1;
SET min_count_to_compile_expression = 0;

-- In-range conversions are exact, and the `Decimal` source directions that stay compiled agree with
-- the interpreter. A `Bool` destination reads the raw value, so a `Decimal` below one is `true`.
SELECT toDecimal32(materialize(7::Int32) + 0, 2),
       toInt64(materialize(42::Decimal64(0))) + 0,
       toFloat64(materialize(42::Decimal64(0))) + 0,
       toUInt32(materialize(5::Decimal32(0))) + 0,
       CAST(materialize(0.5::Decimal64(1)) AS Bool);

-- A value the destination cannot hold raises instead of silently wrapping: the integer to `Decimal`
-- multiply leaves the destination's storage range, the `Decimal` whole part does not fit a narrower
-- destination, and an unsigned destination cannot take a negative whole part. Each conversion needs a
-- compilable neighbour to be compiled at all, which is what `+ 0` and the surrounding arithmetic are
-- for. The second and the last row go through `CAST`, which is the second compilability gate.
SELECT toDecimal32(materialize(30000000::Int32) + 0, 2); -- { serverError DECIMAL_OVERFLOW }
SELECT CAST(materialize(30000000::Int32) + 0 AS Decimal32(2)); -- { serverError DECIMAL_OVERFLOW }
SELECT toInt8(materialize(300::Decimal64(0))) + 0; -- { serverError DECIMAL_OVERFLOW }
SELECT toUInt32(materialize(-5::Decimal32(0))) + 0; -- { serverError DECIMAL_OVERFLOW }
SELECT CAST(materialize(toNullable(300::Decimal64(0))) AS Int8) + 0; -- { serverError DECIMAL_OVERFLOW }

-- Every row above is a value oracle, so all of them would still pass if the conversions that agree
-- with the interpreter silently stopped being compiled. The shapes below pin which of them compiles.
-- Each has exactly one compilable child, so a declined conversion leaves nothing to compile, while
-- the control is plain arithmetic that compiles whatever the conversion rule says.
SELECT toInt64(materialize(42::Decimal64(0))) + 0 FROM numbers(2)
    SETTINGS compile_expressions = 1, min_count_to_compile_expression = 0, log_comment = '05153_kept' FORMAT Null;
SELECT toFloat64(materialize(42::Decimal64(0))) + 0 FROM numbers(2)
    SETTINGS compile_expressions = 1, min_count_to_compile_expression = 0, log_comment = '05153_float' FORMAT Null;
SELECT CAST(materialize(0.5::Decimal64(1)) AS Bool) AND materialize(true) FROM numbers(2)
    SETTINGS compile_expressions = 1, min_count_to_compile_expression = 0, log_comment = '05153_bool' FORMAT Null;
SELECT toDecimal32(materialize(7::Int32) + 0, 2) FROM numbers(2)
    SETTINGS compile_expressions = 1, min_count_to_compile_expression = 0, log_comment = '05153_declined' FORMAT Null;
SELECT materialize(2.0) + materialize(0.0) + materialize(1.0) FROM numbers(2)
    SETTINGS compile_expressions = 1, min_count_to_compile_expression = 0, log_comment = '05153_control' FORMAT Null;

SYSTEM FLUSH LOGS query_log;

WITH shapes AS
(
    SELECT log_comment, argMax(ProfileEvents['CompiledFunctionExecute'] > 0, event_time_microseconds) AS compiled
    FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment LIKE '05153_%'
    GROUP BY log_comment
)
-- The control keeps the first three columns honest in a build without the embedded compiler, where
-- every shape is interpreted and an absolute assertion would go green on nothing being compiled.
SELECT
    (SELECT compiled FROM shapes WHERE log_comment = '05153_kept')
        = (SELECT compiled FROM shapes WHERE log_comment = '05153_control'),
    (SELECT compiled FROM shapes WHERE log_comment = '05153_float')
        = (SELECT compiled FROM shapes WHERE log_comment = '05153_control'),
    (SELECT compiled FROM shapes WHERE log_comment = '05153_bool')
        = (SELECT compiled FROM shapes WHERE log_comment = '05153_control'),
    (SELECT compiled FROM shapes WHERE log_comment = '05153_declined') = 0;
