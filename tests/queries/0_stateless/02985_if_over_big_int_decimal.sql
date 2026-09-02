select sumIf(number::Int128, number % 10 == 0) from numbers(1000);
select sumIf(number::UInt128, number % 10 == 0) from numbers(1000);
select sumIf(number::Int256, number % 10 == 0) from numbers(1000);
select sumIf(number::UInt256, number % 10 == 0) from numbers(1000);
select sumIf(number::Decimal128(3), number % 10 == 0) from numbers(1000);
select sumIf(number::Decimal256(3), number % 10 == 0) from numbers(1000);

-- Test when the condition is neither 0 nor 1
select sumIf(number::Int128, number % 10) from numbers(1000);
select sumIf(number::UInt128, number % 10) from numbers(1000);
select sumIf(number::Int256, number % 10) from numbers(1000);
select sumIf(number::UInt256, number % 10) from numbers(1000);
select sumIf(number::Decimal128(3), number % 10) from numbers(1000);
select sumIf(number::Decimal256(3), number % 10) from numbers(1000);
