SELECT accurateCast(1e35, 'UInt32'); -- { serverError 70 }
SELECT accurateCast(1e35, 'UInt64'); -- { serverError 70 }
SELECT accurateCast(1e35, 'UInt128'); -- { serverError 70 }
SELECT accurateCast(1e35, 'UInt256'); -- { serverError 70 }

SELECT accurateCast(1e19, 'UInt64');
SELECT accurateCast(1e19, 'UInt128');
SELECT accurateCast(1e19, 'UInt256');
SELECT accurateCast(1e20, 'UInt64'); -- { serverError 70 }
SELECT accurateCast(1e20, 'UInt128'); -- { serverError 70 }
SELECT accurateCast(1e20, 'UInt256'); -- { serverError 70 }

SELECT accurateCast(1e19, 'Int64'); -- { serverError 70 }
SELECT accurateCast(1e19, 'Int128');
SELECT accurateCast(1e19, 'Int256');
