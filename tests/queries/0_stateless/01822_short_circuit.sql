set use_short_circuit_function_evaluation = 1;

select if(number > 0, intDiv(number + 100, number), throwIf(number)) from numbers(10);
select multiIf(number == 0, 0, number == 1, intDiv(1, number), number == 2, intDiv(1, number - 1), number == 3, intDiv(1, number - 2), intDiv(1, number - 3)) from numbers(10);
select number != 0 and intDiv(1, number) == 0 and number != 2 and intDiv(1, number - 2) == 0 from numbers(10);
select number == 0 or intDiv(1, number) != 0 or number == 2 or intDiv(1, number - 2) != 0 from numbers(10);

select count() from (select if(number >= 0, number, sleep(1)) from numbers(10000000));

select if(number % 5 == 0, toInt8OrZero(toString(number)), toInt8OrZero(toString(number + 1))) from numbers(20);
select if(number % 5 == 0, toUInt8OrZero(toString(number)), toUInt8OrZero(toString(number + 1))) from numbers(20);
select if(number % 5 == 0, toInt32OrZero(toString(number)), toInt32OrZero(toString(number + 1))) from numbers(20);
select if(number % 5 == 0, toUInt32OrZero(toString(number)), toUInt32OrZero(toString(number + 1))) from numbers(20);
select if(number % 5 == 0, toInt64OrZero(toString(number)), toInt64OrZero(toString(number + 1))) from numbers(20);
select if(number % 5 == 0, toUInt64OrZero(toString(number)), toUInt64OrZero(toString(number + 1))) from numbers(20);
select if(number % 5 == 0, toInt128OrZero(toString(number)), toInt128OrZero(toString(number + 1))) from numbers(20);
select if(number % 5 == 0, toUInt128OrZero(toString(number)), toUInt128OrZero(toString(number + 1))) from numbers(20);
select if(number % 5 == 0, toInt256OrZero(toString(number)), toInt256OrZero(toString(number + 1))) from numbers(20);
select if(number % 5 == 0, toUInt256OrZero(toString(number)), toUInt256OrZero(toString(number + 1))) from numbers(20);

select if(number % 5 == 0, toFloat32OrZero(toString(number)), toFloat32OrZero(toString(number + 1))) from numbers(20);
select if(number % 5 == 0, toFloat64OrZero(toString(number)), toFloat64OrZero(toString(number + 1))) from numbers(20);

select if(number % 5 == 0, repeat(toString(number), 2), repeat(toString(number + 1), 2)) from numbers(20);
select if(number % 5 == 0, toFixedString(toString(number + 10), 2), toFixedString(toString(number + 11), 2)) from numbers(20);

select if(number % 5 == 0, toDateOrZero(toString(number)), toDateOrZero(toString(number + 1))) from numbers(20);
select if(number % 5 == 0, toDateTimeOrZero(toString(number * 10000)), toDateTimeOrZero(toString((number + 1) * 10000))) from numbers(20);

select if(number % 5 == 0, toDecimal32OrZero(toString(number), 5), toDecimal32OrZero(toString(number + 1), 5)) from numbers(20);
select if(number % 5 == 0, toDecimal64OrZero(toString(number), 5), toDecimal64OrZero(toString(number + 1), 5)) from numbers(20);
select if(number % 5 == 0, toDecimal128OrZero(toString(number), 5), toDecimal128OrZero(toString(number + 1), 5)) from numbers(20);
select if(number % 5 == 0, toDecimal256OrZero(toString(number), 5), toDecimal256OrZero(toString(number + 1), 5)) from numbers(20);

select if(number % 5 == 0, range(number), range(number + 1)) from numbers(20);
select if(number % 5 == 0, replicate(toString(number), range(number)), replicate(toString(number), range(number + 1))) from numbers(20);

select number > 0 and 5 and intDiv(100, number) from numbers(5);
select number > 0 and Null and intDiv(100, number) from numbers(5);
select number == 0 or 5 or intDiv(100, number) from numbers(5);
select multiIf(number % 2 != 0, intDiv(10, number % 2), 5, intDiv(10, 1 - number % 2), intDiv(10, number)) from numbers(5);

select if(number != 0, 5 * (1 + intDiv(100, number)), exp(log(throwIf(number) + 10))) from numbers(5);
select if(number % 2, 5 * (1 + intDiv(100, number + 1)), 3 + 10 * intDiv(100, intDiv(100, number + 1))) from numbers(10);

select sum(number) FROM numbers(10) WHERE number != 0 and 3 % number and number != 1 and intDiv(1, number - 1) > 0;

