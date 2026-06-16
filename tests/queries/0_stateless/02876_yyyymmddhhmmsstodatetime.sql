SET session_timezone = 'UTC'; -- no time zone randomization, please

-----------------------------------------------------------
SELECT '--- YYYYMMDDToDateTime';

SELECT 'Invalid input types are rejected';
SELECT YYYYMMDDhhmmssToDateTime(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT YYYYMMDDhhmmssToDateTime(toDate('2023-09-11')); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT YYYYMMDDhhmmssToDateTime(toDate32('2023-09-11')); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT YYYYMMDDhhmmssToDateTime(toDateTime('2023-09-11 12:18:00')); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT YYYYMMDDhhmmssToDateTime(toDateTime64('2023-09-11 12:18:00', 3)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT YYYYMMDDhhmmssToDateTime('2023-09-11'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT YYYYMMDDhhmmssToDateTime(20230911134254, 3); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT YYYYMMDDhhmmssToDateTime(20230911134254, 'invalid tz'); -- { serverError BAD_ARGUMENTS }
SELECT YYYYMMDDhhmmssToDateTime(20230911134254, 'Europe/Berlin', 'bad'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

SELECT 'Result type is DateTime';
SELECT toTypeName(YYYYMMDDhhmmssToDateTime(19910824));
SELECT toTypeName(YYYYMMDDhhmmssToDateTime(cast(19910824 AS Nullable(UInt64))));
--
SELECT 'Check correctness, integer arguments';
SELECT YYYYMMDDhhmmssToDateTime(19691231595959);
SELECT YYYYMMDDhhmmssToDateTime(19700101000000);
SELECT YYYYMMDDhhmmssToDateTime(20200229111111); -- leap day
SELECT YYYYMMDDhhmmssToDateTime(20230911150505);
SELECT YYYYMMDDhhmmssToDateTime(21060207062815);
SELECT YYYYMMDDhhmmssToDateTime(21060207062816);
SELECT YYYYMMDDhhmmssToDateTime(9223372036854775807); -- huge value

SELECT 'Check correctness, float arguments';
SELECT YYYYMMDDhhmmssToDateTime(19691231595959.1);
SELECT YYYYMMDDhhmmssToDateTime(19700101000000.1);
SELECT YYYYMMDDhhmmssToDateTime(20200229111111.1); -- leap day
SELECT YYYYMMDDhhmmssToDateTime(20230911150505.1);
SELECT YYYYMMDDhhmmssToDateTime(21060207062815.1);
SELECT YYYYMMDDhhmmssToDateTime(21060207062816.1);
SELECT YYYYMMDDhhmmssToDateTime(NaN); -- { serverError BAD_ARGUMENTS }
SELECT YYYYMMDDhhmmssToDateTime(Inf); -- { serverError BAD_ARGUMENTS }
SELECT YYYYMMDDhhmmssToDateTime(-Inf); -- { serverError BAD_ARGUMENTS }

SELECT 'Check correctness, decimal arguments';
SELECT YYYYMMDDhhmmssToDateTime(toDecimal64(19691231595959.1, 5));
SELECT YYYYMMDDhhmmssToDateTime(toDecimal64(19700101000000.1, 5));
SELECT YYYYMMDDhhmmssToDateTime(toDecimal64(20200229111111.1, 5)); -- leap day
SELECT YYYYMMDDhhmmssToDateTime(toDecimal64(20230911150505.1, 5));
SELECT YYYYMMDDhhmmssToDateTime(toDecimal64(21060207062815.1, 5));
SELECT YYYYMMDDhhmmssToDateTime(toDecimal64(21060207062816.1, 5));

SELECT 'Special cases';
SELECT YYYYMMDDhhmmssToDateTime(-20230911111111); -- negative
SELECT YYYYMMDDhhmmssToDateTime(110); -- invalid everything
SELECT YYYYMMDDhhmmssToDateTime(999999999999999999); -- huge value
SELECT YYYYMMDDhhmmssToDateTime(15001113111111); -- year out of range
SELECT YYYYMMDDhhmmssToDateTime(35001113111111); -- year out of range
SELECT YYYYMMDDhhmmssToDateTime(20231620111111); -- invalid month
SELECT YYYYMMDDhhmmssToDateTime(20230020111111); -- invalid month
SELECT YYYYMMDDhhmmssToDateTime(20230940111111); -- invalid day
SELECT YYYYMMDDhhmmssToDateTime(20230900111111); -- invalid day
SELECT YYYYMMDDhhmmssToDateTime(20230228111111); -- leap day when there is none
SELECT YYYYMMDDhhmmssToDateTime(True);
SELECT YYYYMMDDhhmmssToDateTime(False);
SELECT YYYYMMDDhhmmssToDateTime(NULL);
SELECT YYYYMMDDhhmmssToDateTime(yyyymmdd) FROM (SELECT 19840121 AS yyyymmdd UNION ALL SELECT 20230911 AS yyyymmdd) ORDER BY yyyymmdd; -- non-const

-----------------------------------------------------------
SELECT '--- YYYYMMDDToDateTime64';

SELECT 'Invalid input types are rejected';
SELECT YYYYMMDDhhmmssToDateTime64(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT YYYYMMDDhhmmssToDateTime64(toDate('2023-09-11')); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT YYYYMMDDhhmmssToDateTime64(toDate32('2023-09-11')); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT YYYYMMDDhhmmssToDateTime64(toDateTime('2023-09-11 12:18:00')); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT YYYYMMDDhhmmssToDateTime64(toDateTime64('2023-09-11 12:18:00', 3)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT YYYYMMDDhhmmssToDateTime64('2023-09-11'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT YYYYMMDDhhmmssToDateTime64('2023-09-11', 'invalid precision'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT YYYYMMDDhhmmssToDateTime64(20230911134254, 3, 3); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT YYYYMMDDhhmmssToDateTime64(20230911134254, 3, 'invalid tz'); -- { serverError BAD_ARGUMENTS }
SELECT YYYYMMDDhhmmssToDateTime64(20230911134254, 3, 'Europe/Berlin', 'no more args'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

SELECT 'Result type is DateTime';
SELECT toTypeName(YYYYMMDDhhmmssToDateTime64(19910824));
SELECT toTypeName(YYYYMMDDhhmmssToDateTime64(19910824, 5));
SELECT toTypeName(YYYYMMDDhhmmssToDateTime64(cast(19910824 AS Nullable(UInt64))));

SELECT 'Check correctness, integer arguments';
SELECT YYYYMMDDhhmmssToDateTime64(189912315959);
SELECT YYYYMMDDhhmmssToDateTime64(19000101000000);
SELECT YYYYMMDDhhmmssToDateTime64(20200229111111); -- leap day
SELECT YYYYMMDDhhmmssToDateTime64(20230911150505);
SELECT YYYYMMDDhhmmssToDateTime64(22991231235959);
SELECT YYYYMMDDhhmmssToDateTime64(23000101000000);
-- SELECT YYYYMMDDhhmmssToDateTime64(9223372036854775807); -- huge value, commented out because on ARM, the rounding is slightly different

SELECT 'Check correctness, float arguments';
SELECT YYYYMMDDhhmmssToDateTime64(189912315959.1);
SELECT YYYYMMDDhhmmssToDateTime64(19000101000000.1);
SELECT YYYYMMDDhhmmssToDateTime64(20200229111111.1); -- leap day
SELECT YYYYMMDDhhmmssToDateTime64(20230911150505.1);
SELECT YYYYMMDDhhmmssToDateTime64(22991231235959.1);
SELECT YYYYMMDDhhmmssToDateTime64(23000101000000.1);
SELECT YYYYMMDDhhmmssToDateTime64(NaN); -- { serverError BAD_ARGUMENTS }
SELECT YYYYMMDDhhmmssToDateTime64(Inf); -- { serverError BAD_ARGUMENTS }
SELECT YYYYMMDDhhmmssToDateTime64(-Inf); -- { serverError BAD_ARGUMENTS }

SELECT 'Check correctness, decimal arguments';
SELECT YYYYMMDDhhmmssToDateTime64(toDecimal64(189912315959.1, 5));
SELECT YYYYMMDDhhmmssToDateTime64(toDecimal64(19000101000000.1, 5));
SELECT YYYYMMDDhhmmssToDateTime64(toDecimal64(20200229111111.1, 5)); -- leap day
SELECT YYYYMMDDhhmmssToDateTime64(toDecimal64(20230911150505.1, 5));
SELECT YYYYMMDDhhmmssToDateTime64(toDecimal64(22991231235959.1, 5));
SELECT YYYYMMDDhhmmssToDateTime64(toDecimal64(23000101000000.1, 5));

SELECT 'Special cases';
SELECT YYYYMMDDhhmmssToDateTime64(-20230911111111); -- negative
SELECT YYYYMMDDhhmmssToDateTime64(110); -- invalid everything
SELECT YYYYMMDDhhmmssToDateTime64(999999999999999999); -- huge value
SELECT YYYYMMDDhhmmssToDateTime64(15001113111111); -- year out of range
SELECT YYYYMMDDhhmmssToDateTime64(35001113111111); -- year out of range
SELECT YYYYMMDDhhmmssToDateTime64(20231620111111); -- invalid month
SELECT YYYYMMDDhhmmssToDateTime64(20230020111111); -- invalid month
SELECT YYYYMMDDhhmmssToDateTime64(20230940111111); -- invalid day
SELECT YYYYMMDDhhmmssToDateTime64(20230900111111); -- invalid day
SELECT YYYYMMDDhhmmssToDateTime64(20230228111111); -- leap day when there is none
SELECT YYYYMMDDhhmmssToDateTime64(True);
SELECT YYYYMMDDhhmmssToDateTime64(False);
SELECT YYYYMMDDhhmmssToDateTime64(NULL);
SELECT YYYYMMDDhhmmssToDateTime64(yyyymmdd) FROM (SELECT 19840121 AS yyyymmdd UNION ALL SELECT 20230911 AS yyyymmdd) ORDER BY yyyymmdd; -- non-const
