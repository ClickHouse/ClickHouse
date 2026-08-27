-- Aggregate and window function parameters read their `Field` directly, with `safeGet<Float64>` or a
-- strict `Int64`/`UInt64` type check, so a deferred number literal must never reach them.

SET enable_analyzer = 1;

SELECT quantile(0.5)(number), quantile(5e-1)(number), quantile(0.50)(number), quantile(1e-1)(number) FROM numbers(10);
SELECT quantiles(0.5, 0.9)(number), quantileGK(100, 0.5)(number), quantileTDigest(0.5)(number) FROM numbers(10);
SELECT groupArray(10)(number), groupArray(0x5)(number), groupArraySample(3, 12345)(number) FROM numbers(20);
SELECT windowFunnel(1)(toDateTime(number), number = 1, number = 2) FROM numbers(10);
SELECT quantile(0.5)(number) OVER () FROM numbers(3);
SELECT quantileIf(0.5)(number, number > 2), quantileArray(0.5)([number]) FROM numbers(10);
SELECT arrayReduce('quantile(0.5)', range(10));

-- A parameter that is not a positive integer is still rejected, whatever the spelling.
SELECT groupArray(2.5)(number) FROM numbers(3); -- { serverError BAD_ARGUMENTS }
SELECT groupArray(-1)(number) FROM numbers(3); -- { serverError BAD_ARGUMENTS }
SELECT groupArray(18446744073709551616)(number) FROM numbers(3); -- { serverError BAD_ARGUMENTS }

SET enable_analyzer = 0;

SELECT quantile(0.5)(number), quantile(5e-1)(number), quantile(0.50)(number), quantile(1e-1)(number) FROM numbers(10);
SELECT quantiles(0.5, 0.9)(number), quantileGK(100, 0.5)(number), quantileTDigest(0.5)(number) FROM numbers(10);
SELECT groupArray(10)(number), groupArray(0x5)(number), groupArraySample(3, 12345)(number) FROM numbers(20);
SELECT windowFunnel(1)(toDateTime(number), number = 1, number = 2) FROM numbers(10);
SELECT quantile(0.5)(number) OVER () FROM numbers(3);
SELECT quantileIf(0.5)(number, number > 2), quantileArray(0.5)([number]) FROM numbers(10);
SELECT arrayReduce('quantile(0.5)', range(10));

SELECT groupArray(2.5)(number) FROM numbers(3); -- { serverError BAD_ARGUMENTS }
SELECT groupArray(-1)(number) FROM numbers(3); -- { serverError BAD_ARGUMENTS }
SELECT groupArray(18446744073709551616)(number) FROM numbers(3); -- { serverError BAD_ARGUMENTS }
