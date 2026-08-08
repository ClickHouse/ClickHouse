-- avg over Date/Time types converts the Float64 average back to the native integer type.
-- The exact average always lies within the range of the inputs, but the Float64 computation is
-- inexact: for ticks near the bounds of Int64 it can land on 2^63 exactly, and the cast was
-- undefined behavior (caught by UBSan). The conversion must saturate instead.

SELECT avg(x) FROM (SELECT fromUnixTimestamp64Nano(9223372036854775807, 'UTC') AS x FROM numbers(2));
SELECT avg(x) FROM (SELECT fromUnixTimestamp64Nano(-9223372036854775808, 'UTC') AS x FROM numbers(2));
SELECT avg(x) FROM (SELECT fromUnixTimestamp64Nano(if(number % 2 = 0, 9223372036854775807, 9223372036854775806), 'UTC') AS x FROM numbers(100));
