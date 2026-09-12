-- Tests the vectorized implementation of clamp for numeric arguments

SELECT 'All three branches over a multi-row block';
SELECT clamp(materialize(toUInt32(5)), materialize(toUInt32(10)), materialize(toUInt32(20))) AS x, toTypeName(x);
SELECT clamp(materialize(toUInt32(15)), materialize(toUInt32(10)), materialize(toUInt32(20))) AS x, toTypeName(x);
SELECT clamp(materialize(toUInt32(25)), materialize(toUInt32(10)), materialize(toUInt32(20))) AS x, toTypeName(x);
SELECT number, clamp(number, 3, 6) FROM numbers(10);

SELECT 'Type promotion across arguments';
SELECT clamp(materialize(toInt8(-5)), materialize(toUInt8(1)), materialize(toUInt32(10))) AS x, toTypeName(x);
SELECT clamp(materialize(toInt32(-5)), materialize(toFloat32(0.5)), materialize(toFloat32(10.5))) AS x, toTypeName(x);

SELECT 'NaN value clamps to max, NaN max leaves the value unchanged';
SELECT clamp(materialize(nan), materialize(1.0), materialize(10.0));
SELECT clamp(materialize(5.0), materialize(1.0), materialize(nan));
SELECT clamp(materialize(nan), materialize(1.0), materialize(nan));

SELECT 'NaN min is greater than max';
SELECT clamp(materialize(5.0), materialize(nan), materialize(10.0)); -- { serverError BAD_ARGUMENTS }
SELECT clamp(materialize(5.0), materialize(nan), materialize(nan));

SELECT 'Min greater than max in a single row of a block';
SELECT clamp(materialize(toUInt32(5)), materialize(toUInt32(10)), materialize(toUInt32(1))); -- { serverError BAD_ARGUMENTS }
SELECT clamp(number, number, if(number = 5, 0, 100)) FROM numbers(10); -- { serverError BAD_ARGUMENTS }

SELECT 'Infinities';
SELECT clamp(materialize(-inf), materialize(0.0), materialize(inf));
SELECT clamp(materialize(inf), materialize(0.0), materialize(1.0));

SELECT 'BFloat16';
SET allow_experimental_bfloat16_type = 1;
SELECT clamp(materialize(toBFloat16(5)), materialize(toBFloat16(1)), materialize(toBFloat16(2))) AS x, toTypeName(x);
SELECT clamp(materialize(toBFloat16(nan)), materialize(toBFloat16(1)), materialize(toBFloat16(2))) AS x, toTypeName(x);

SELECT 'Big integers';
SELECT clamp(materialize(toInt256(-1)), materialize(toInt256(5)), materialize(toInt256(10))) AS x, toTypeName(x);
SELECT clamp(materialize(toUInt128(100)), materialize(toUInt128(5)), materialize(toUInt128(10))) AS x, toTypeName(x);

SELECT 'Nullable arguments go through the default NULL implementation';
SELECT clamp(materialize(toNullable(toUInt32(25))), materialize(toUInt32(10)), materialize(toUInt32(20))) AS x, toTypeName(x);
SELECT clamp(materialize(CAST(NULL, 'Nullable(UInt32)')), materialize(toUInt32(10)), materialize(toUInt32(20))) AS x, toTypeName(x);
