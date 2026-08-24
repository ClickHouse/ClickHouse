set short_circuit_function_evaluation = 'enable';
set convert_query_to_cnf = 0;

select if(number > 0, intDiv(number + 100, number), throwIf(number)) from numbers(10);
select multiIf(number == 0, 0, number == 1, intDiv(1, number), number == 2, intDiv(1, number - 1), number == 3, intDiv(1, number - 2), intDiv(1, number - 3)) from numbers(10);
select number != 0 and intDiv(1, number) == 0 and number != 2 and intDiv(1, number - 2) == 0 from numbers(10);
select number == 0 or intDiv(1, number) != 0 or number == 2 or intDiv(1, number - 2) != 0 from numbers(10);

select count() from (select if(number >= 0, number, sleep(1)) from numbers(10000000));


select if(number % 5 == 0, toInt8OrZero(toString(number)), toInt8OrZero(toString(number + 1))) from numbers(20);
select if(number % 5 == 0, toInt8OrZero(toString(number)), Null) from numbers(20);
select if(number % 5 == 0, Null, toInt8OrZero(toString(number))) from numbers(20);
select if(number % 5, Null, toInt8OrZero(toString(number))) from numbers(20);

select if(number % 5 == 0, toUInt8OrZero(toString(number)), toUInt8OrZero(toString(number + 1))) from numbers(20);
select if(number % 5 == 0, toUInt8OrZero(toString(number)), Null) from numbers(20);
select if(number % 5 == 0, Null, toUInt8OrZero(toString(number))) from numbers(20);
select if(number % 5, Null, toUInt8OrZero(toString(number))) from numbers(20);

select if(number % 5 == 0, toInt32OrZero(toString(number)), toInt32OrZero(toString(number + 1))) from numbers(20);
select if(number % 5 == 0, toInt32OrZero(toString(number)), Null) from numbers(20);
select if(number % 5 == 0, Null, toInt32OrZero(toString(number))) from numbers(20);
select if(number % 5, Null, toInt32OrZero(toString(number))) from numbers(20);

select if(number % 5 == 0, toUInt32OrZero(toString(number)), toUInt32OrZero(toString(number + 1))) from numbers(20);
select if(number % 5 == 0, toUInt32OrZero(toString(number)), Null) from numbers(20);
select if(number % 5 == 0, Null, toUInt32OrZero(toString(number))) from numbers(20);
select if(number % 5, Null, toUInt32OrZero(toString(number))) from numbers(20);

select if(number % 5 == 0, toInt64OrZero(toString(number)), toInt64OrZero(toString(number + 1))) from numbers(20);
select if(number % 5 == 0, toInt64OrZero(toString(number)), Null) from numbers(20);
select if(number % 5 == 0, Null, toInt64OrZero(toString(number))) from numbers(20);
select if(number % 5, Null, toInt64OrZero(toString(number))) from numbers(20);

select if(number % 5 == 0, toUInt64OrZero(toString(number)), toUInt64OrZero(toString(number + 1))) from numbers(20);
select if(number % 5 == 0, toUInt64OrZero(toString(number)), Null) from numbers(20);
select if(number % 5 == 0, Null, toUInt64OrZero(toString(number))) from numbers(20);
select if(number % 5, Null, toUInt64OrZero(toString(number))) from numbers(20);

select if(number % 5 == 0, toInt128OrZero(toString(number)), toInt128OrZero(toString(number + 1))) from numbers(20);
select if(number % 5 == 0, toInt128OrZero(toString(number)), Null) from numbers(20);
select if(number % 5 == 0, Null, toInt128OrZero(toString(number))) from numbers(20);
select if(number % 5, Null, toInt128OrZero(toString(number))) from numbers(20);

select if(number % 5 == 0, toUInt128OrZero(toString(number)), toUInt128OrZero(toString(number + 1))) from numbers(20);
select if(number % 5 == 0, toUInt128OrZero(toString(number)), Null) from numbers(20);
select if(number % 5 == 0, Null, toUInt128OrZero(toString(number))) from numbers(20);
select if(number % 5, Null, toUInt128OrZero(toString(number))) from numbers(20);

select if(number % 5 == 0, toInt256OrZero(toString(number)), toInt256OrZero(toString(number + 1))) from numbers(20);
select if(number % 5 == 0, toInt256OrZero(toString(number)), Null) from numbers(20);
select if(number % 5 == 0, Null, toInt256OrZero(toString(number))) from numbers(20);
select if(number % 5, Null, toInt256OrZero(toString(number))) from numbers(20);

select if(number % 5 == 0, toUInt256OrZero(toString(number)), toUInt256OrZero(toString(number + 1))) from numbers(20);
select if(number % 5 == 0, toUInt256OrZero(toString(number)), Null) from numbers(20);
select if(number % 5 == 0, Null, toUInt256OrZero(toString(number))) from numbers(20);
select if(number % 5, Null, toUInt256OrZero(toString(number))) from numbers(20);

select if(number % 5 == 0, toFloat32OrZero(toString(number)), toFloat32OrZero(toString(number + 1))) from numbers(20);
select if(number % 5 == 0, toFloat32OrZero(toString(number)), Null) from numbers(20);
select if(number % 5 == 0, Null, toFloat32OrZero(toString(number))) from numbers(20);
select if(number % 5, Null, toFloat32OrZero(toString(number))) from numbers(20);

select if(number % 5 == 0, toFloat64OrZero(toString(number)), toFloat64OrZero(toString(number + 1))) from numbers(20);
select if(number % 5 == 0, toFloat64OrZero(toString(number)), Null) from numbers(20);
select if(number % 5 == 0, Null, toFloat64OrZero(toString(number))) from numbers(20);
select if(number % 5, Null, toFloat64OrZero(toString(number))) from numbers(20);

select if(number % 5 == 0, repeat(toString(number), 2), repeat(toString(number + 1), 2)) from numbers(20);
select if(number % 5 == 0, repeat(toString(number), 2), Null) from numbers(20);
select if(number % 5 == 0, Null, repeat(toString(number), 2)) from numbers(20);
select if(number % 5, Null, repeat(toString(number), 2)) from numbers(20);

select if(number % 5 == 0, toFixedString(toString(number + 10), 2), toFixedString(toString(number + 11), 2)) from numbers(20);
select if(number % 5 == 0, toFixedString(toString(number + 10), 2), Null) from numbers(20);
select if(number % 5 == 0, Null, toFixedString(toString(number + 10), 2)) from numbers(20);
select if(number % 5, Null, toFixedString(toString(number + 10), 2)) from numbers(20);

select if(number % 5 == 0, toDateOrZero(toString(number)), toDateOrZero(toString(number + 1))) from numbers(20);
select if(number % 5 == 0, toDateOrZero(toString(number)), Null) from numbers(20);
select if(number % 5 == 0, Null, toDateOrZero(toString(number))) from numbers(20);
select if(number % 5, Null, toDateOrZero(toString(number))) from numbers(20);

select if(number % 5 == 0, toDateTimeOrZero(toString(number * 10000), 'UTC'), toDateTimeOrZero(toString((number + 1) * 10000), 'UTC')) from numbers(20);
select if(number % 5 == 0, toDateTimeOrZero(toString(number * 10000), 'UTC'), Null) from numbers(20);
select if(number % 5 == 0, Null, toDateTimeOrZero(toString(number * 10000), 'UTC')) from numbers(20);
select if(number % 5, Null, toDateTimeOrZero(toString(number * 10000), 'UTC')) from numbers(20);

select if(number % 5 == 0, toDecimal32OrZero(toString(number), 5), toDecimal32OrZero(toString(number + 1), 5)) from numbers(20);
select if(number % 5 == 0, toDecimal32OrZero(toString(number), 5), Null) from numbers(20);
select if(number % 5 == 0, Null, toDecimal32OrZero(toString(number), 5)) from numbers(20);
select if(number % 5, Null, toDecimal32OrZero(toString(number), 5)) from numbers(20);

select if(number % 5 == 0, toDecimal64OrZero(toString(number), 5), toDecimal64OrZero(toString(number + 1), 5)) from numbers(20);
select if(number % 5 == 0, toDecimal64OrZero(toString(number), 5), Null) from numbers(20);
select if(number % 5 == 0, Null, toDecimal64OrZero(toString(number), 5)) from numbers(20);
select if(number % 5, Null, toDecimal64OrZero(toString(number), 5)) from numbers(20);

select if(number % 5 == 0, toDecimal128OrZero(toString(number), 5), toDecimal128OrZero(toString(number + 1), 5)) from numbers(20);
select if(number % 5 == 0, toDecimal128OrZero(toString(number), 5), Null) from numbers(20);
select if(number % 5 == 0, Null, toDecimal128OrZero(toString(number), 5)) from numbers(20);
select if(number % 5, Null, toDecimal128OrZero(toString(number), 5)) from numbers(20);

select if(number % 5 == 0, toDecimal256OrZero(toString(number), 5), toDecimal256OrZero(toString(number + 1), 5)) from numbers(20);
select if(number % 5 == 0, toDecimal256OrZero(toString(number), 5), Null) from numbers(20);
select if(number % 5 == 0, Null, toDecimal256OrZero(toString(number), 5)) from numbers(20);
select if(number % 5, Null, toDecimal256OrZero(toString(number), 5)) from numbers(20);

select if(number % 5 == 0, range(number), range(number + 1)) from numbers(20);
select if(number % 5 == 0, replicate(toString(number), range(number)), replicate(toString(number), range(number + 1))) from numbers(20);

select number > 0 and 5 and intDiv(100, number) from numbers(5);
select number > 0 and Null and intDiv(100, number) from numbers(5);
select number == 0 or 5 or intDiv(100, number) from numbers(5);
select multiIf(number % 2 != 0, intDiv(10, number % 2), 5, intDiv(10, 1 - number % 2), intDiv(10, number)) from numbers(5);

select if(number != 0, 5 * (1 + intDiv(100, number)), toInt32(exp(log(throwIf(number) + 10)))) from numbers(5);
select if(number % 2, 5 * (1 + intDiv(100, number + 1)), 3 + 10 * intDiv(100, intDiv(100, number + 1))) from numbers(10);

select sum(number) FROM numbers(10) WHERE number != 0 and 3 % number and number != 1 and intDiv(1, number - 1) > 0;
select multiIf(0, 1, intDiv(number % 2, 1), 2, 0, 3, 1, number + 10, 2) from numbers(10);

select toTypeName(toString(number)) from numbers(5);
select toColumnTypeName(toString(number)) from numbers(5);

select toTypeName(toInt64OrZero(toString(number))) from numbers(5);
select toColumnTypeName(toInt64OrZero(toString(number))) from numbers(5);

select toTypeName(toDecimal32OrZero(toString(number), 5)) from numbers(5);
select toColumnTypeName(toDecimal32OrZero(toString(number), 5)) from numbers(5);

select if(if(number > 0, intDiv(42, number), 0), intDiv(42, number), 8) from numbers(5);
select if(number > 0, intDiv(42, number), 0), if(number = 0, 0, intDiv(42, number)) from numbers(5);

select Null or isNull(intDiv(number, 1)) from numbers(5);

set compile_expressions = 1;
select if(number > 0, intDiv(42, number), 1) from numbers(5);
select if(number > 0, intDiv(42, number), 1) from numbers(5);
select if(number > 0, intDiv(42, number), 1) from numbers(5);
select if(number > 0, intDiv(42, number), 1) from numbers(5);

select if(number > 0, 42 / toDecimal32(number, 2), 0) from numbers(5);
select if(number = 0, 0, toDecimal32(42, 2) / number) from numbers(5);
select if(isNull(x), Null, 42 / x) from (select CAST(materialize(Null), 'Nullable(Decimal32(2))') as x);
select if(isNull(x), Null, x / 0) from (select CAST(materialize(Null), 'Nullable(Decimal32(2))') as x);

select if(isNull(x), Null, intDiv(42, x)) from (select CAST(materialize(Null), 'Nullable(Int64)') as x);

select number % 2 and toLowCardinality(number) from numbers(5);
select number % 2 or toLowCardinality(number) from numbers(5);
select if(toLowCardinality(number) % 2, number, number + 1) from numbers(10);
select multiIf(toLowCardinality(number) % 2, number, number + 1) from numbers(10);

