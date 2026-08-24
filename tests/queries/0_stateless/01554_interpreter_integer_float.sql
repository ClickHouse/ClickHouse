SELECT reinterpretAsFloat32(CAST(123456 AS UInt32));
SELECT reinterpretAsUInt32(CAST(1.23456 AS Float32));
SELECT reinterpretAsFloat32(CAST(123456 AS Int32));
SELECT reinterpretAsInt32(CAST(1.23456 AS Float32));
SELECT reinterpretAsFloat64(CAST(123456 AS UInt64));
SELECT reinterpretAsUInt64(CAST(1.23456 AS Float64));
SELECT reinterpretAsFloat64(CAST(123456 AS Int64));
SELECT reinterpretAsInt64(CAST(1.23456 AS Float64));
