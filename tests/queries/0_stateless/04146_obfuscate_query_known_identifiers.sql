-- Function names, aggregate function names, table function names, formats, data types,
-- and codecs must be preserved by `obfuscateQuery` / `obfuscateQueryWithSeed`,
-- mirroring the behaviour of `clickhouse-format --obfuscate`.

SELECT obfuscateQueryWithSeed('SELECT count(*), sum(x), avg(y) FROM t', 1) LIKE '%count%' AS keep_count,
       obfuscateQueryWithSeed('SELECT count(*), sum(x), avg(y) FROM t', 1) LIKE '%sum%'   AS keep_sum,
       obfuscateQueryWithSeed('SELECT count(*), sum(x), avg(y) FROM t', 1) LIKE '%avg%'   AS keep_avg;

SELECT obfuscateQueryWithSeed('SELECT arrayMap(x -> x + 1, a) FROM t', 2) LIKE '%arrayMap%' AS keep_array_map;

SELECT obfuscateQueryWithSeed('SELECT toUInt64(x) FROM numbers(10)', 3) LIKE '%toUInt64%' AS keep_to_uint64,
       obfuscateQueryWithSeed('SELECT toUInt64(x) FROM numbers(10)', 3) LIKE '%numbers%'  AS keep_numbers_table_function;

-- Case-insensitive table functions (e.g. `VALUES`, `FORMAT`) and aliases must also be preserved.
SELECT obfuscateQueryWithSeed('SELECT * FROM VALUES(''a UInt8'', 1, 2, 3)', 6) LIKE '%VALUES%' AS keep_uppercase_values,
       obfuscateQueryWithSeed('SELECT * FROM url(''https://example.com'', JSONEachRow)', 7) LIKE '%url%' AS keep_url_table_function;

SELECT obfuscateQueryWithSeed('CREATE TABLE t (x UInt64, y String) ENGINE = MergeTree ORDER BY x', 4) LIKE '%MergeTree%' AS keep_engine,
       obfuscateQueryWithSeed('CREATE TABLE t (x UInt64, y String) ENGINE = MergeTree ORDER BY x', 4) LIKE '%UInt64%'    AS keep_uint64,
       obfuscateQueryWithSeed('CREATE TABLE t (x UInt64, y String) ENGINE = MergeTree ORDER BY x', 4) LIKE '%String%'    AS keep_string;

SELECT obfuscateQueryWithSeed('SELECT * FROM t FORMAT JSONEachRow', 5) LIKE '%JSONEachRow%' AS keep_format;

-- Same query inside `obfuscateQuery` (random seed) is non-deterministic but should still preserve known names.
SELECT countIf(s LIKE '%count%') = count() AS keep_count_random
FROM (SELECT obfuscateQuery('SELECT count(*) FROM t') AS s FROM numbers(5));
