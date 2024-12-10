SELECT divideOrNull(1 , CAST(NULL, 'Nullable(Float32)'));
SELECT divideOrNull(materialize(1), CAST(NULL, 'Nullable(Float32)'));

SELECT divideOrNull(1 , 0);
SELECT divideOrNull(materialize(1) , 0);
SELECT divideOrNull(1 , materialize(0));
SELECT divideOrNull(materialize(1) , materialize(0));

SELECT divideOrNull(1, CAST(materialize(0.0), 'Nullable(Float32)'));
SELECT divideOrNull(materialize(1), CAST(materialize(0.0), 'Nullable(Float32)'));

SELECT divideOrNull(1.1, CAST(1, 'Nullable(Float32)'));
SELECT divideOrNull(materialize(11.1), CAST(1, 'Nullable(Float32)'));
SELECT divideOrNull(10, CAST(materialize(1.2), 'Nullable(Float32)'));
SELECT divideOrNull(10, CAST(materialize(1), 'Nullable(Int128)'));
SELECT divideOrNull(CAST(16.0, 'Float32'), CAST(materialize(9), 'Nullable(Int128)'));


SELECT divideOrNull(CAST(16, 'Int64'), CAST(materialize(9), 'Nullable(Int128)'));
SELECT divideOrNull(CAST(16, 'Int32'), CAST(materialize(9), 'Nullable(Int128)'));
SELECT divideOrNull(CAST(16, 'Int8'), CAST(materialize(9), 'Nullable(Int128)'));
SELECT divideOrNull(CAST(16, 'Int128'), CAST(materialize(9), 'Nullable(Int128)'));
SELECT divideOrNull(CAST(16, 'UInt256'), CAST(materialize(9), 'Nullable(UInt128)'));
SELECT divideOrNull(CAST(16, 'UInt256'), CAST(materialize(9), 'Nullable(UInt128)'));

SELECT divideOrNull(toDecimal32(16.2, 2), toDecimal32(0.0, 1));
SELECT divideOrNull(toDecimal32(16.2, 2), materialize(toDecimal32(0.0, 1)));
SELECT divideOrNull(materialize(toDecimal32(16.2, 2)), toDecimal32(0.0, 1));
SELECT divideOrNull(materialize(toDecimal32(16.2, 2)), materialize(toDecimal32(0.0, 1)));

SELECT divideOrNull(toDecimal32(16.2, 2), 0.0);
SELECT divideOrNull(toDecimal32(16.2, 2), materialize(0.0));
SELECT divideOrNull(materialize(toDecimal32(16.2, 2)), materialize(0.0));

SELECT tupleDivideOrNull((15, 10, 5), (0, 0, 0));
SELECT tupleDivideOrNull((15, 10, 5), (5, 0, 0));
SELECT tupleDivideByNumberOrNull((15, 10, 5), 5);
SELECT tupleDivideByNumberOrNull((15, 10, 5), 0);
