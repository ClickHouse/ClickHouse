SELECT roundAge(0);
SELECT roundAge(18);
SELECT roundAge(25);
SELECT roundAge(35);
SELECT roundAge(45);
SELECT roundAge(55);
SELECT roundAge(56);

SELECT roundAge(nan), roundAge(-nan);
SELECT roundAge(inf), roundAge(-inf);
SELECT roundAge(-1), roundAge(-55);
SELECT roundAge(-128::Int8), roundAge(-32768::Int16), roundAge(-2147483648::Int32);
SELECT roundAge(0.5), roundAge(0.9999), roundAge(17.9), roundAge(54.5);
SELECT roundAge(17.999::Float32), roundAge(18.0::Float32), roundAge(-0.0::Float32);
SELECT roundAge(18446744073709551615::UInt64), roundAge(170141183460469231731687303715884105727::Int128);
SELECT roundAge(toNullable(18)), roundAge(toNullable(NULL));
SELECT roundAge(toLowCardinality(17)), roundAge(toLowCardinality(18));
SELECT sum(roundAge(number)) FROM numbers(100);
