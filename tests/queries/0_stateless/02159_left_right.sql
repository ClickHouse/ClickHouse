-- { echo }

SELECT left('Hello', 3);
SELECT left('Hello', -3);
SELECT left('Hello', 5);
SELECT left('Hello', -5);
SELECT left('Hello', 6);
SELECT left('Hello', -6);
SELECT left('Hello', 0);
SELECT left('Hello', NULL);

SELECT left(materialize('Привет'), 4);
SELECT LEFT('Привет', -4);
SELECT left(toNullable('Привет'), 12);
SELECT left(toNullable('Привет'), -12);
SELECT lEFT('Привет', -12);
SELECT left(materialize(toNullable('Привет')), 13);
SELECT left(materialize(toNullable('Привет')), -13);
SELECT left(materialize(toNullable('Привет')), -4);
SELECT left('Привет', -13);
SELECT Left('Привет', 0);
SELECT left('Привет', NULL);

SELECT leftUTF8('Привет', 4);
SELECT leftUTF8('Привет', -4);
SELECT leftUTF8('Привет', 12);
SELECT leftUTF8('Привет', -12);
SELECT leftUTF8('Привет', 13);
SELECT leftUTF8('Привет', -13);
SELECT leftUTF8('Привет', 0);
SELECT leftUTF8('Привет', NULL);

SELECT left('Hello', number) FROM numbers(10);
SELECT leftUTF8('Привет', number) FROM numbers(10);
SELECT left('Hello', -number) FROM numbers(10);
SELECT leftUTF8('Привет', -number) FROM numbers(10);

SELECT leftUTF8('Привет', number % 3 = 0 ? NULL : (number % 2 ? toInt64(number) : -number)) FROM numbers(10);
SELECT leftUTF8(number < 5 ? 'Hello' : 'Привет', number % 3 = 0 ? NULL : (number % 2 ? toInt64(number) : -number)) FROM numbers(10);

SELECT right('Hello', 3);
SELECT right('Hello', -3);
SELECT right('Hello', 5);
SELECT right('Hello', -5);
SELECT right('Hello', 6);
SELECT right('Hello', -6);
SELECT right('Hello', 0);
SELECT right('Hello', NULL);

SELECT right(materialize('Hello'), -3);
SELECT left(materialize('Hello'), -3);
SELECT right(materialize('Hello'), -5);
SELECT left(materialize('Hello'), -5);
SELECT rightUTF8(materialize('Hello'), -3);
SELECT leftUTF8(materialize('Hello'), -3);
SELECT rightUTF8(materialize('Hello'), -5);
SELECT leftUTF8(materialize('Hello'), -5);

SELECT RIGHT(materialize('Привет'), 4);
SELECT RIGHT(materialize('Привет'), -4);
SELECT right('Привет', -4);
SELECT Right(toNullable('Привет'), 12);
SELECT Right(toNullable('Привет'), -12);
SELECT right('Привет', -12);
SELECT rIGHT(materialize(toNullable('Привет')), 13);
SELECT rIGHT(materialize(toNullable('Привет')), -13);
SELECT right('Привет', -13);
SELECT rIgHt('Привет', 0);
SELECT RiGhT('Привет', NULL);

SELECT rightUTF8('Привет', 4);
SELECT rightUTF8('Привет', -4);
SELECT rightUTF8('Привет', 12);
SELECT rightUTF8('Привет', -12);
SELECT rightUTF8('Привет', 13);
SELECT rightUTF8('Привет', -13);
SELECT rightUTF8('Привет', 0);
SELECT rightUTF8('Привет', NULL);

SELECT right('Hello', number) FROM numbers(10);
SELECT rightUTF8('Привет', number) FROM numbers(10);
SELECT right('Hello', -number) FROM numbers(10);
SELECT rightUTF8('Привет', -number) FROM numbers(10);

SELECT rightUTF8('Привет', number % 3 = 0 ? NULL : (number % 2 ? toInt64(number) : -number)) FROM numbers(10);
SELECT rightUTF8(number < 5 ? 'Hello' : 'Привет', number % 3 = 0 ? NULL : (number % 2 ? toInt64(number) : -number)) FROM numbers(10);

-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/99725
-- right() with INT64_MIN must raise an overflow exception
-- Const-length path (string is non-const so useDefaultImplementationForConstants does not apply)
SELECT right(materialize('Hello'), -9223372036854775808); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT rightUTF8(materialize('Привет'), -9223372036854775808); -- { serverError ARGUMENT_OUT_OF_BOUND }
-- Dynamic-length path: bitNot(INT64_MAX) == INT64_MIN
SELECT right('Hello', materialize(bitNot(toInt64(9223372036854775807)))); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT rightUTF8('Привет', materialize(bitNot(toInt64(9223372036854775807)))); -- { serverError ARGUMENT_OUT_OF_BOUND }
-- substring with negative offset=INT64_MIN: abs(offset) overflows in sliceDynamicOffsetBoundedImpl
-- materialize() prevents constant-folding so the dynamic code path is taken
SELECT substring('Hello', materialize(bitNot(toInt64(9223372036854775807))), -1); -- { serverError ARGUMENT_OUT_OF_BOUND }
-- substring with large positive offset: adjustment=element_size-(offset-1) is very negative,
-- and size+adjustment underflows Int64
SELECT substring('Hello', materialize(toInt64(9223372036854775807)), -8); -- { serverError ARGUMENT_OUT_OF_BOUND }
