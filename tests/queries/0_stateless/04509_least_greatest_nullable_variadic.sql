-- Tests the vectorized implementation of least/greatest for Nullable and variadic (3+) numeric arguments

SELECT 'Nullable arguments skip NULLs, return NULL only if all arguments are NULL';
SELECT least(materialize(CAST(NULL, 'Nullable(UInt32)')), materialize(CAST(5, 'Nullable(UInt32)'))) AS x, toTypeName(x);
SELECT greatest(materialize(CAST(NULL, 'Nullable(UInt32)')), materialize(CAST(5, 'Nullable(UInt32)'))) AS x, toTypeName(x);
SELECT least(materialize(CAST(NULL, 'Nullable(UInt32)')), materialize(CAST(NULL, 'Nullable(UInt32)'))) AS x, toTypeName(x);
SELECT greatest(materialize(CAST(NULL, 'Nullable(UInt32)')), materialize(CAST(NULL, 'Nullable(UInt32)'))) AS x, toTypeName(x);

SELECT 'Mixed Nullable and non-Nullable arguments';
SELECT least(materialize(CAST(NULL, 'Nullable(UInt32)')), materialize(toUInt32(3))) AS x, toTypeName(x);
SELECT greatest(materialize(CAST(7, 'Nullable(UInt32)')), materialize(toUInt32(3))) AS x, toTypeName(x);

SELECT 'Three and four arguments';
SELECT least(materialize(toUInt32(3)), materialize(toUInt32(1)), materialize(toUInt32(2))) AS x, toTypeName(x);
SELECT greatest(materialize(toUInt32(3)), materialize(toUInt32(1)), materialize(toUInt32(2))) AS x, toTypeName(x);
SELECT least(materialize(toUInt32(4)), materialize(toUInt32(3)), materialize(toUInt32(2)), materialize(toUInt32(1))) AS x, toTypeName(x);
SELECT greatest(materialize(toUInt32(4)), materialize(toUInt32(3)), materialize(toUInt32(2)), materialize(toUInt32(1))) AS x, toTypeName(x);

SELECT 'Type promotion across arguments';
SELECT least(materialize(toUInt8(200)), materialize(toInt8(-1)), materialize(toUInt32(100000))) AS x, toTypeName(x);
SELECT greatest(materialize(toUInt8(200)), materialize(toInt8(-1)), materialize(toUInt32(100000))) AS x, toTypeName(x);
SELECT least(materialize(toInt32(-5)), materialize(toFloat32(2.5))) AS x, toTypeName(x);
SELECT least(materialize(toNullable(toInt16(-5))), materialize(toUInt8(3)), materialize(CAST(NULL, 'Nullable(Int32)'))) AS x, toTypeName(x);

SELECT 'NaN loses against numbers and NULLs lose against NaN';
SELECT least(materialize(nan), materialize(1.0)), greatest(materialize(nan), materialize(1.0));
SELECT least(materialize(1.0), materialize(nan), materialize(2.0)), greatest(materialize(1.0), materialize(nan), materialize(2.0));
SELECT least(materialize(CAST(NULL, 'Nullable(Float64)')), materialize(toNullable(nan))) AS x, toTypeName(x);
SELECT greatest(materialize(toNullable(nan)), materialize(CAST(NULL, 'Nullable(Float64)'))) AS x, toTypeName(x);
SELECT least(materialize(toNullable(nan)), materialize(toNullable(1.0))), greatest(materialize(toNullable(nan)), materialize(toNullable(1.0)));

SELECT 'BFloat16: NaN before and after finite values, variadic and Nullable';
SET allow_experimental_bfloat16_type = 1;
SELECT least(materialize(toBFloat16(nan)), materialize(toBFloat16(1)), materialize(toBFloat16(2))) AS l, greatest(materialize(toBFloat16(nan)), materialize(toBFloat16(1)), materialize(toBFloat16(2))) AS g, toTypeName(l);
SELECT least(materialize(toBFloat16(1)), materialize(toBFloat16(2)), materialize(toBFloat16(nan))) AS l, greatest(materialize(toBFloat16(1)), materialize(toBFloat16(2)), materialize(toBFloat16(nan))) AS g, toTypeName(l);
SELECT least(materialize(toBFloat16(nan)), materialize(toBFloat16(nan)), materialize(toBFloat16(nan))) AS l, greatest(materialize(toBFloat16(nan)), materialize(toBFloat16(nan)), materialize(toBFloat16(nan))) AS g;
SELECT least(materialize(toNullable(toBFloat16(nan))), materialize(toNullable(toBFloat16(1.5)))) AS l, greatest(materialize(toNullable(toBFloat16(1.5))), materialize(toNullable(toBFloat16(nan)))) AS g, toTypeName(l);
SELECT least(materialize(CAST(NULL, 'Nullable(BFloat16)')), materialize(toNullable(toBFloat16(nan)))) AS x, toTypeName(x);

SELECT 'Infinities';
SELECT least(materialize(inf), materialize(1.0), materialize(-inf)), greatest(materialize(inf), materialize(1.0), materialize(-inf));

SELECT 'Big integers';
SELECT least(materialize(toNullable(toUInt128(7))), materialize(CAST(NULL, 'Nullable(UInt128)'))) AS x, toTypeName(x);
SELECT least(materialize(toNullable(toInt256(-1))), materialize(toNullable(toInt256(5))), materialize(CAST(NULL, 'Nullable(Int256)'))) AS x, toTypeName(x);

SELECT 'Only-NULL arguments are ignored';
SELECT least(materialize(toUInt32(5)), NULL, materialize(toUInt32(3))) AS x, toTypeName(x);
SELECT greatest(NULL, materialize(toUInt32(5)), NULL) AS x, toTypeName(x);

SELECT 'Legacy NULL behavior';
SELECT least(materialize(CAST(NULL, 'Nullable(UInt32)')), materialize(CAST(5, 'Nullable(UInt32)'))) AS x, toTypeName(x) SETTINGS least_greatest_legacy_null_behavior = 1;
SELECT least(materialize(toNullable(toUInt32(2))), materialize(toNullable(toUInt32(5))), materialize(toNullable(toUInt32(3)))) AS x, toTypeName(x) SETTINGS least_greatest_legacy_null_behavior = 1;
SELECT greatest(materialize(toNullable(toUInt32(2))), materialize(toNullable(toUInt32(5))), NULL) AS x, toTypeName(x) SETTINGS least_greatest_legacy_null_behavior = 1;

SELECT 'All NULL combinations over four Nullable arguments';
SELECT least(a, b, c, d) AS l, greatest(a, b, c, d) AS g, toTypeName(l)
FROM
(
    SELECT
        if(number % 2 = 0, NULL, toUInt32(number)) AS a,
        if(intDiv(number, 2) % 2 = 0, NULL, toUInt32(100 - number)) AS b,
        if(intDiv(number, 4) % 2 = 0, NULL, toUInt32(number * 3)) AS c,
        if(intDiv(number, 8) % 2 = 0, NULL, toUInt32(50)) AS d
    FROM numbers(16)
)
ORDER BY ALL;
