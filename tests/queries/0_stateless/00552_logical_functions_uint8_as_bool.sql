
-- Test that UInt8 type is processed correctly as bool

SELECT 
    toUInt8(bitAnd(number, 4)) AS a,
    toUInt8(bitAnd(number, 2)) AS b,
    toUInt8(bitAnd(number, 1)) AS c,
    a AND b AND c AS AND,
    a OR b OR c AS OR
FROM numbers(8)
;