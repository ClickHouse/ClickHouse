SELECT analysisOfVariance(number, number % 2) FROM numbers(10) FORMAT Null;
SELECT analysisOfVariance(number :: Decimal32(5), number % 2) FROM numbers(10) FORMAT Null;
SELECT analysisOfVariance(number :: Decimal256(5), number % 2) FROM numbers(10) FORMAT Null;

SELECT analysisOfVariance(1.11, -20); -- { serverError BAD_ARGUMENTS }
SELECT analysisOfVariance(1.11, 20 :: UInt128); -- { serverError BAD_ARGUMENTS }
SELECT analysisOfVariance(1.11, 9000000000000000); -- { serverError BAD_ARGUMENTS }

SELECT analysisOfVariance(number, number % 2), analysisOfVariance(100000000000000000000., number % 65535) FROM numbers(1048575);
