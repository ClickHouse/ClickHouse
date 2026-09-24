-- `Decimal` to integer and to float conversions must agree with the interpreter when they are
-- JIT-compiled, at a non-zero scale and for negative values. `+ 0` is what makes each conversion
-- eligible for compilation: alone it has no compilable neighbour and stays interpreted.
SET compile_expressions = 1;
SET min_count_to_compile_expression = 0;

SELECT toInt32(materialize(-5.1234::Decimal32(4))) + 0,
       toInt64(materialize(-123.45678901::Decimal64(8))) + 0,
       toInt64(materialize(-0.5::Decimal64(1))) + 0,
       toInt128(materialize(-999.123456789012345678::Decimal128(18))) + 0,
       toFloat64(materialize(-123.45::Decimal64(2))) + 0,
       CAST(materialize(-42.5::Decimal64(1)) AS Int64) + 0;

SELECT toInt32(materialize(-5.1234::Decimal32(4))) + 0,
       toInt64(materialize(-123.45678901::Decimal64(8))) + 0,
       toInt64(materialize(-0.5::Decimal64(1))) + 0,
       toInt128(materialize(-999.123456789012345678::Decimal128(18))) + 0,
       toFloat64(materialize(-123.45::Decimal64(2))) + 0,
       CAST(materialize(-42.5::Decimal64(1)) AS Int64) + 0
SETTINGS compile_expressions = 0;
