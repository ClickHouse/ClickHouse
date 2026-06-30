SET allow_experimental_nullable_array_type = 1;

-- Nullable(Array) * LowCardinality(UInt8) should work like array * scalar
SELECT throwIf(
    CAST([2] AS Nullable(Array(Int32))) * toLowCardinality(toUInt8(3)) != [6],
    'LowCardinality operand not handled'
) FORMAT Null;

-- intDiv with LowCardinality denominator
SELECT throwIf(
    intDiv(CAST([10] AS Nullable(Array(Int32))), toLowCardinality(toUInt8(2))) != [5],
    'LowCardinality denominator not handled'
) FORMAT Null;
