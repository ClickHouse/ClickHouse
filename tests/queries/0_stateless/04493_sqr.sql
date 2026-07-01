SELECT sqr(5) = 25;
SELECT sqr(-5) = 25;
SELECT sqr(toInt8(-12)) = multiply(toInt8(-12), toInt8(-12)), toTypeName(sqr(toInt8(-12))) = toTypeName(multiply(toInt8(-12), toInt8(-12)));

SELECT sqr(toUInt8(5)) = multiply(toUInt8(5), toUInt8(5)), toTypeName(sqr(toUInt8(5))) = toTypeName(multiply(toUInt8(5), toUInt8(5)));
SELECT sqr(toFloat32(1.5)) = multiply(toFloat32(1.5), toFloat32(1.5)), toTypeName(sqr(toFloat32(1.5))) = toTypeName(multiply(toFloat32(1.5), toFloat32(1.5)));
SELECT sqr(toFloat64(-1.5)) = multiply(toFloat64(-1.5), toFloat64(-1.5)), toTypeName(sqr(toFloat64(-1.5))) = toTypeName(multiply(toFloat64(-1.5), toFloat64(-1.5)));
SELECT sqr(toDecimal32('1.20', 2)) = multiply(toDecimal32('1.20', 2), toDecimal32('1.20', 2)), toTypeName(sqr(toDecimal32('1.20', 2))) = toTypeName(multiply(toDecimal32('1.20', 2), toDecimal32('1.20', 2)));
SELECT sqr(toDecimal32('-1.20', 2)) = multiply(toDecimal32('-1.20', 2), toDecimal32('-1.20', 2)), toTypeName(sqr(toDecimal32('-1.20', 2))) = toTypeName(multiply(toDecimal32('-1.20', 2), toDecimal32('-1.20', 2)));

SELECT sqr(inf) = inf;
SELECT sqr(-inf) = inf;
SELECT isNaN(sqr(nan));

SELECT sqr(toNullable(toInt32(7))) = 49, toTypeName(sqr(toNullable(toInt32(7)))) = toTypeName(multiply(toNullable(toInt32(7)), toNullable(toInt32(7))));
SELECT isNull(sqr(CAST(NULL, 'Nullable(Int32)')));

SELECT toTypeName(sqr(toLowCardinality(number))) = toTypeName(multiply(toLowCardinality(number), toLowCardinality(number))) FROM numbers(1);

SELECT sqr('abc'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
