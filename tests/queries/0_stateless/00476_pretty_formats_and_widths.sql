SET output_format_pretty_color=1;
SELECT toUInt64(round(exp10(number))) AS x, toString(x) AS s FROM system.numbers LIMIT 10 FORMAT Pretty;
SELECT toUInt64(round(exp10(number))) AS x, toString(x) AS s FROM system.numbers LIMIT 10 FORMAT PrettyCompact;
SELECT toUInt64(round(exp10(number))) AS x, toString(x) AS s FROM system.numbers LIMIT 10 FORMAT PrettySpace;
SET max_block_size = 5;
SELECT toUInt64(round(exp10(number))) AS x, toString(x) AS s FROM system.numbers LIMIT 10 FORMAT PrettyCompactMonoBlock;
SELECT '\\''\'' FORMAT Pretty;
SELECT '\\''\'', 1 FORMAT Vertical;
