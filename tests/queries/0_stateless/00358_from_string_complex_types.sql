SELECT CAST('[1, 2, 3]' AS Array(UInt8));
SELECT CAST(toString(range(number)) AS Array(UInt64)), CAST(toString((number, number * 111)) AS Tuple(UInt64, UInt8)) FROM system.numbers LIMIT 10;
