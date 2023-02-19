-- Tags: no-fasttest

SET output_format_write_statistics = 0;
SET extremes = 1;

SET output_format_json_quote_64bit_integers = 1;
SELECT toInt64(0) as i0, toUInt64(0) as u0, toInt64(9223372036854775807) as ip, toInt64(-9223372036854775808) as in, toUInt64(18446744073709551615) as up, [toInt64(0)] as arr, (toUInt64(0), toUInt64(0)) as tuple GROUP BY i0, u0, ip, in, up, arr, tuple WITH TOTALS FORMAT JSON;
SELECT toInt64(0) as i0, toUInt64(0) as u0, toInt64(9223372036854775807) as ip, toInt64(-9223372036854775808) as in, toUInt64(18446744073709551615) as up, [toInt64(0)] as arr, (toUInt64(0), toUInt64(0)) as tuple GROUP BY i0, u0, ip, in, up, arr, tuple WITH TOTALS FORMAT JSONCompact;
SELECT toInt64(0) as i0, toUInt64(0) as u0, toInt64(9223372036854775807) as ip, toInt64(-9223372036854775808) as in, toUInt64(18446744073709551615) as up, [toInt64(0)] as arr, (toUInt64(0), toUInt64(0)) as tuple GROUP BY i0, u0, ip, in, up, arr, tuple WITH TOTALS FORMAT JSONEachRow;

SET output_format_json_quote_64bit_integers = 0;
SELECT toInt64(0) as i0, toUInt64(0) as u0, toInt64(9223372036854775807) as ip, toInt64(-9223372036854775808) as in, toUInt64(18446744073709551615) as up, [toInt64(0)] as arr, (toUInt64(0), toUInt64(0)) as tuple GROUP BY i0, u0, ip, in, up, arr, tuple WITH TOTALS FORMAT JSON;
SELECT toInt64(0) as i0, toUInt64(0) as u0, toInt64(9223372036854775807) as ip, toInt64(-9223372036854775808) as in, toUInt64(18446744073709551615) as up, [toInt64(0)] as arr, (toUInt64(0), toUInt64(0)) as tuple GROUP BY i0, u0, ip, in, up, arr, tuple WITH TOTALS FORMAT JSONCompact;
SELECT toInt64(0) as i0, toUInt64(0) as u0, toInt64(9223372036854775807) as ip, toInt64(-9223372036854775808) as in, toUInt64(18446744073709551615) as up, [toInt64(0)] as arr, (toUInt64(0), toUInt64(0)) as tuple GROUP BY i0, u0, ip, in, up, arr, tuple WITH TOTALS FORMAT JSONEachRow;
