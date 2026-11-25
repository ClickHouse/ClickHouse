-- Parts of the Trino SQL dialect that are available in ClickHouse SQL without enabling the dialect.

SELECT '-- TRY_CAST';
SELECT TRY_CAST('123' AS INTEGER), TRY_CAST('abc' AS INTEGER), try_cast(300 AS UInt8), Try_Cast('1.5', 'Float64');
SELECT toTypeName(TRY_CAST('1' AS BIGINT));
SELECT TRY_CAST(x AS INTEGER) FROM (SELECT arrayJoin(['1', 'x', '3']) AS x);
SELECT TRY_CAST('2020-01-02' AS DATE), TRY_CAST('not a date' AS DATE);
SELECT TRY_CAST('1' AS t AS INTEGER), t;
-- A literal is read as exactly as by `CAST`.
SELECT TRY_CAST(0.1 AS Decimal256(76)) = CAST(0.1 AS Decimal256(76)), TRY_CAST(607668569663131286404589520 AS UInt128), TRY_CAST(0.1, 'Decimal256(76)') = CAST(0.1, 'Decimal256(76)');
SELECT TRY_CAST(-1 AS UInt128), TRY_CAST(1e100 AS Decimal(10, 2));

SELECT '-- ARRAY[...]';
SELECT ARRAY[1, 2, 3], array[], Array['a', 'b'], toTypeName(ARRAY[1, NULL]);
SELECT ARRAY[ARRAY[1, 2], ARRAY[3, 4]], ARRAY[(1, 2), (3, 4)], ARRAY[1, 2][2];
SELECT ARRAY[number, number * 2] FROM numbers(2);
SELECT ARRAY['x'], ARRAY[ARRAY[1]], toTypeName(ARRAY[1]);
SELECT ARRAY[number] FROM numbers(2);
SELECT arrayMap(x -> ARRAY[x], [1, 2]);
-- A column named `array` is still subscripted.
SELECT array[2], ARRAY[1, 2] FROM (SELECT [10, 20] AS array);
SELECT a.array[1] FROM (SELECT [10, 20] AS array) AS a;
SELECT `array`[1] FROM (SELECT [10, 20] AS array);
SELECT arrayMap(array -> array[1], [[5], [6]]);
-- Only the bracket syntax is the constructor, not an explicit `arrayElement` call.
SELECT arrayElement(array, 1); -- { serverError UNKNOWN_IDENTIFIER }
SELECT arrayElement(ARRAY, 1); -- { serverError UNKNOWN_IDENTIFIER }

SELECT '-- Composite types';
SELECT CAST([1, 2] AS ARRAY(INTEGER)) AS x, toTypeName(x);
SELECT CAST(map('a', 1) AS MAP(VARCHAR, BIGINT)) AS x, toTypeName(x);
SELECT CAST([[1, 2]] AS ARRAY(ARRAY(INTEGER))) AS x, toTypeName(x);
DROP TABLE IF EXISTS t_trino_types;
CREATE TABLE t_trino_types (a ARRAY(INTEGER), m MAP(VARCHAR, DOUBLE)) ENGINE = Memory;
SHOW CREATE TABLE t_trino_types;
DROP TABLE t_trino_types;

SELECT '-- Functions';
SELECT starts_with('abc', 'ab'), ends_with('abc', 'bc'), levenshtein_distance('kitten', 'sitting');
SELECT regexp_like('abc', 'b');
SELECT is_nan(nan), is_finite(1.), is_infinite(inf);
SELECT day_of_month(toDate('2024-02-29')), day_of_week(toDate('2024-02-29')), day_of_year(toDate('2024-02-29')),
    week_of_year(toDate('2021-01-01')), year_of_week(toDate('2021-01-01')), last_day_of_month(toDate('2024-02-10'));
SELECT current_timezone() = timezone();
SELECT format_datetime(toDateTime('2024-02-29 13:14:15', 'UTC'), 'yyyy-MM-dd HH:mm:ss');
SELECT arraySort(array_union(ARRAY[1, 2], ARRAY[2, 3]));
SELECT array_position(ARRAY[5, 6], 6);
SELECT slice(ARRAY[1, 2, 3, 4], 2, 2), zip(ARRAY[1, 2], ARRAY['a']);
SELECT map_keys(map('a', 1, 'b', 2)), map_values(map('a', 1, 'b', 2));
SELECT url_extract_host('https://clickhouse.com:8443/docs/x?a=1&b=2#frag'), url_extract_protocol('https://clickhouse.com/');
SELECT url_extract_host('http://user:password@example.com:8080/path?query=value#fragment');
SELECT url_encode('a b&c'), url_decode('a+b%26c');
SELECT bitwise_and(12, 10), bitwise_or(12, 10), bitwise_xor(12, 10);
SELECT to_hex('abc'), from_hex('616263'), to_base32('abc'), from_base32('MFRGG===');
SELECT toTypeName(uuid());

SELECT '-- Aggregate functions';
SELECT arbitrary(1), bool_and(x), bool_or(x), bool_and(x) FILTER (WHERE x) FROM (SELECT arrayJoin([true, false]) AS x);
SELECT bitwise_and_agg(x), bitwise_or_agg(x), bitwise_xor_agg(x) FROM (SELECT arrayJoin([12, 10]) AS x);
