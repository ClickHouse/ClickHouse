SELECT accurateCast(1e35, 'UInt32'); -- { serverError CANNOT_CONVERT_TYPE }
SELECT accurateCast(1e35, 'UInt64'); -- { serverError CANNOT_CONVERT_TYPE }
SELECT accurateCast(1e35, 'UInt128'); -- { serverError CANNOT_CONVERT_TYPE }
SELECT accurateCast(1e35, 'UInt256'); -- { serverError CANNOT_CONVERT_TYPE }

SELECT accurateCast(1e19, 'UInt64');
SELECT accurateCast(1e19, 'UInt128');
SELECT accurateCast(1e19, 'UInt256');
SELECT accurateCast(1e20, 'UInt64'); -- { serverError CANNOT_CONVERT_TYPE }
SELECT accurateCast(1e20, 'UInt128'); -- { serverError CANNOT_CONVERT_TYPE }
SELECT accurateCast(1e20, 'UInt256'); -- { serverError CANNOT_CONVERT_TYPE }

SELECT accurateCast(1e19, 'Int64'); -- { serverError CANNOT_CONVERT_TYPE }
SELECT accurateCast(1e19, 'Int128');
SELECT accurateCast(1e19, 'Int256');
