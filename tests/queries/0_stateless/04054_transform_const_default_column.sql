-- The default argument of `transform` can be a full column on some blocks
-- but a ColumnConst on others (e.g. when NULL propagation makes it constant).
-- The function must handle this without a type mismatch exception in insertFrom.
SELECT transform(toString(number), ['a'], [1], if(0, 1, plus(NULL, number)) * 1) FROM numbers(1);
SELECT transform(toString(number), ['a'], [1], if(0, 1, plus(NULL, number)) * 1) FROM numbers(10);
SELECT transform(toString(number), ['0'], [1], if(0, 1, plus(NULL, number)) * 1) FROM numbers(3);
SELECT transform(number, [999], [1], CAST(NULL, 'Nullable(UInt16)') * number) FROM numbers(1);
