-- https://github.com/ClickHouse/ClickHouse/issues/102051
-- sz_find_skylake OOB read when needle is all null bytes

-- n_length <= 3: tail returns OOB pointer without validation
SELECT countSubstrings(toString(number), '\0\0') FROM numbers(100) WHERE countSubstrings(toString(number), '\0\0') != 0;
SELECT position(toString(number), '\0\0\0') FROM numbers(100) WHERE position(toString(number), '\0\0\0') != 0;

-- n_length > 3: sz_equal_skylake reads past haystack boundary
SELECT countSubstrings(toString(number), '\0\0\0\0') FROM numbers(100) WHERE countSubstrings(toString(number), '\0\0\0\0') != 0;
SELECT position(toString(number), '\0\0\0\0\0') FROM numbers(100) WHERE position(toString(number), '\0\0\0\0\0') != 0;
