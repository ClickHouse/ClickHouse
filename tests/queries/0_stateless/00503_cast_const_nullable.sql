SELECT CAST(1 AS Nullable(UInt8)) AS id WHERE id = CAST(1 AS Nullable(UInt8));
SELECT CAST(1 AS Nullable(UInt8)) AS id WHERE id = 1;
SELECT NULL == CAST(toUInt8(0) AS Nullable(UInt8));
