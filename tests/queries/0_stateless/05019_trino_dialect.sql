SET allow_experimental_trino_dialect = 1;
SET dialect = 'trino';

SELECT '-- syntax: ARRAY literals';
SELECT ARRAY[1, 2, 3];
SELECT ARRAY['a', 'b'][2];
SELECT ARRAY[ARRAY[1], ARRAY[2, 3]];

SELECT '-- syntax: ROW constructor and ROW type';
SELECT ROW(1, 'a');
SELECT CAST(ROW(1, 'ab') AS ROW(x INTEGER, y VARCHAR)).y;

SELECT '-- syntax: TRY_CAST';
SELECT TRY_CAST('42' AS BIGINT), TRY_CAST('4b' AS BIGINT);
SELECT TRY_CAST('x' AS INTEGER) IS NULL;

SELECT '-- syntax: VALUES';
VALUES 1, 2;
SELECT x, y FROM (VALUES (1, 'a'), (2, 'b')) AS t(x, y) ORDER BY x;

SELECT '-- syntax: OFFSET, LIMIT, FETCH';
SELECT x FROM (VALUES 1, 2, 3, 4, 5) AS t(x) ORDER BY x OFFSET 1 LIMIT 2;
SELECT x FROM (VALUES 1, 2, 3) AS t(x) ORDER BY x OFFSET 2;
SELECT x FROM (VALUES 1, 2, 3) AS t(x) ORDER BY x FETCH FIRST 2 ROWS ONLY;
SELECT x FROM (VALUES 1, 2, 3) AS t(x) ORDER BY x OFFSET 1 FETCH NEXT 1 ROW ONLY;
SELECT x FROM (VALUES 1, 2) AS t(x) ORDER BY x LIMIT ALL;

SELECT '-- syntax: UNNEST';
SELECT x FROM UNNEST(ARRAY[10, 20]) AS t(x);
SELECT n, x FROM (SELECT 1 AS n, ARRAY[2, 3] AS arr) CROSS JOIN UNNEST(arr) AS t(x);
SELECT x, o FROM (SELECT ARRAY['a', 'b'] AS arr) CROSS JOIN UNNEST(arr) WITH ORDINALITY AS t(x, o);
SELECT k, v FROM (SELECT map(ARRAY['a', 'b'], ARRAY[1, 2]) AS m) CROSS JOIN UNNEST(m) AS t(k, v) ORDER BY k;
SELECT a, b FROM UNNEST(ARRAY[1, 2, 3], ARRAY['x', 'y']) AS t(a, b);

SELECT '-- strings are code-point based';
SELECT length('héllo'), upper('hello'), substr('héllo', 2, 2);
SELECT strpos('héllo', 'llo'), reverse('ab');
SELECT split('a,b,c', ','), split_part('a,b,c', ',', 2), split_part('a,b,c', ',', 5) IS NULL;
SELECT concat_ws('-', 'a', NULL, 'b');
SELECT replace('abcabc', 'b'), replace('abcabc', 'b', 'x');
SELECT format('%s=%d', 'a', 5);
SELECT '-- backslash is a regular character';
SELECT length('a\nb');

SELECT '-- arrays and lambdas';
SELECT transform(ARRAY[1, 2, 3], x -> x * 2);
SELECT filter(ARRAY[1, 2, 3, 4], x -> x % 2 = 0);
SELECT any_match(ARRAY[1, 2], x -> x > 1), all_match(ARRAY[1, 2], x -> x > 1), none_match(ARRAY[1, 2], x -> x > 2);
SELECT reduce(ARRAY[1, 2, 3], CAST(0 AS BIGINT), (s, x) -> CAST(s + x AS BIGINT), s -> s);
SELECT cardinality(ARRAY[1, 2, 3]), contains(ARRAY[1, 2], 2), array_position(ARRAY[10, 20], 20);
SELECT element_at(ARRAY[1, 2], 2), element_at(ARRAY[1, 2], 5) IS NULL;
SELECT sequence(1, 5), sequence(5, 1), sequence(1, 9, 3);
SELECT repeat('v', 3), slice(ARRAY[1, 2, 3, 4], 2, 2);
SELECT zip(ARRAY[1, 2, 3], ARRAY['a', 'b']);
SELECT array_join(ARRAY[1, NULL, 2], ','), array_join(ARRAY[1, NULL, 2], ',', 'N');
SELECT flatten(ARRAY[ARRAY[1], ARRAY[2, 3]]), array_distinct(ARRAY[1, 1, 2]);
SELECT array_first(ARRAY[1, 2, 3]), array_last(ARRAY[1, 2, 3], x -> x < 3);

SELECT '-- maps';
SELECT map(ARRAY['a', 'b'], ARRAY[1, 2]);
SELECT map_keys(map(ARRAY['k'], ARRAY['v'])), map_values(map(ARRAY['k'], ARRAY['v']));
SELECT map_concat(map(ARRAY['a'], ARRAY[1]), map(ARRAY['a', 'b'], ARRAY[10, 20]));
SELECT element_at(map(ARRAY['a'], ARRAY[1]), 'a'), element_at(map(ARRAY['a'], ARRAY[1]), 'b') IS NULL;
SELECT map_filter(map(ARRAY['a', 'b'], ARRAY[1, 2]), (k, v) -> v > 1);
SELECT map_entries(map(ARRAY['a'], ARRAY[1])), map_from_entries(ARRAY[ROW('a', 1)]);
SELECT transform_values(map(ARRAY['a'], ARRAY[1]), (k, v) -> v * 10);

SELECT '-- date and time';
SELECT date_diff('day', DATE '2020-01-01', DATE '2020-03-05');
SELECT date_diff('year', DATE '2019-12-31', DATE '2020-01-01');
SELECT date_add('day', 5, DATE '2020-01-01');
SELECT date_trunc('month', DATE '2020-02-15');
SELECT from_unixtime(0, 'UTC');
SELECT to_unixtime(from_unixtime(998456645, 'UTC'));
SELECT format_datetime(TIMESTAMP '2020-01-02 03:04:05', 'yyyy/MM/dd');
SELECT date_format(TIMESTAMP '2020-01-02 03:04:05', '%Y/%m/%d');
SELECT parse_datetime('2020-01-02', 'yyyy-MM-dd');
SELECT day_of_week(DATE '2020-01-06'), day_of_year(DATE '2020-02-01'), week(DATE '2017-01-01'), year_of_week(DATE '2017-01-01');
SELECT last_day_of_month(DATE '2020-02-15');

SELECT '-- JSON';
SELECT json_extract_scalar('{"a": {"b": 3}}', '$.a.b');
SELECT json_array_length('[1, 2, 3]');

SELECT '-- regular expressions';
SELECT regexp_like('Hello', '^H'), regexp_extract('a1b2', '[0-9]');
SELECT regexp_replace('a1b2', '[0-9]'), regexp_replace('new york', '(\w)(\w*)', '$1$2!');
SELECT regexp_split('one,two;three', '[,;]'), regexp_count('a1b2c3', '[0-9]');

SELECT '-- math and misc';
SELECT round(log(2, 8)), round(ln(exp(1))), truncate(3.79), mod(7, 3);
SELECT is_nan(nan()), is_finite(1.0), is_infinite(infinity());
SELECT cosine_similarity(ARRAY[1.0, 0.0], ARRAY[1.0, 0.0]);
SELECT bitwise_and(12, 10), bitwise_or(12, 10), bitwise_left_shift(1, 4);
SELECT to_hex(from_hex('414243'));
SELECT to_big_endian_64(1) = from_hex('0000000000000001');
SELECT random() BETWEEN 0 AND 1, random(10) BETWEEN 0 AND 9;
SELECT if(1 > 2, 'yes'), if(1 < 2, 'yes', 'no');
SELECT url_extract_host('https://clickhouse.com/docs?x=1'), url_extract_port('https://clickhouse.com/docs') IS NULL;

SELECT '-- aggregate functions';
SELECT approx_distinct(x), count_if(x > 1), bool_and(x > 0), bool_or(x > 2) FROM (VALUES 1, 2, 3) AS t(x);
SELECT arbitrary(x) FROM (VALUES 7) AS t(x);
SELECT array_sort(array_agg(DISTINCT x)) FROM (VALUES 2, 1, 2) AS t(x);
SELECT approx_percentile(x, 0.5) FROM (VALUES 1, 2, 3, 4, 5) AS t(x);
SELECT approx_percentile(x, ARRAY[0.0, 1.0]) FROM (VALUES 1, 2, 3) AS t(x);
SELECT min(x, 2), max(x, 2) FROM (VALUES 3, 1, 2) AS t(x);
SELECT max_by(x, y), min_by(x, y) FROM (VALUES ('a', 1), ('b', 2)) AS t(x, y);
SELECT histogram(x) FROM (VALUES 1, 1, 2) AS t(x);
SELECT map_agg(k, v) FROM (VALUES ('a', 1)) AS t(k, v);
SELECT listagg(x, '+') FROM (VALUES 'a', 'b') AS t(x);
SELECT round(geometric_mean(x)) FROM (VALUES 2.0, 8.0) AS t(x);
SELECT count(DISTINCT x) FROM (VALUES 1, 1, 2) AS t(x);
SELECT sum(x) FILTER (WHERE x > 1) FROM (VALUES 1, 2, 3) AS t(x);

SELECT '-- window functions';
SELECT x, lag(x) OVER (ORDER BY x), lead(x) OVER (ORDER BY x) FROM (VALUES 1, 2) AS t(x) ORDER BY x;
SELECT x, row_number() OVER (ORDER BY x), first_value(x) OVER (ORDER BY x) FROM (VALUES 3, 4) AS t(x) ORDER BY x;

SELECT '-- INSERT round trip';
CREATE TABLE t_05019 (x Int64, s String) ENGINE = Memory;
INSERT INTO t_05019 VALUES (1, 'a'), (2, 'b');
INSERT INTO t_05019 SELECT x + 10, upper(s) FROM t_05019;
SELECT x, s FROM t_05019 ORDER BY x;
DROP TABLE t_05019;

SELECT '-- literals: TIMESTAMP with fraction and zone, DECIMAL, .5 without leading zero';
SELECT TIMESTAMP '2022-11-01 09:08:07.321';
SELECT TIMESTAMP '2024-01-01 12:00:00 Asia/Tokyo';
SELECT date_diff('millisecond', TIMESTAMP '2022-10-31 09:08:07.198', TIMESTAMP '2022-11-01 09:08:07.321');
SELECT DECIMAL '123.45', CAST(DECIMAL '123.45' AS VARCHAR);
SELECT 3 BETWEEN .06 - 0.01 AND .5 * 10;

SELECT '-- more standard forms';
SELECT trim('!' FROM '!foo!'), trim(LEADING FROM '  abcd');
SELECT bitwise_not(19);
SELECT translate('Palhoça', 'ç', 'c');
SELECT current_timestamp(3) >= now() - 5, localtimestamp(6) >= now() - 5;
SELECT TIMESTAMP '2020-01-01 00:00:00' AT LOCAL = TIMESTAMP '2020-01-01 00:00:00';

SELECT '-- BETWEEN SYMMETRIC';
SELECT 3 BETWEEN SYMMETRIC 6 AND 2, 3 BETWEEN SYMMETRIC 2 AND 6, 1 BETWEEN SYMMETRIC 6 AND 2;
SELECT 3 BETWEEN ASYMMETRIC 2 AND 6, 3 BETWEEN ASYMMETRIC 6 AND 2;
SELECT 7 NOT BETWEEN SYMMETRIC 6 AND 2;
SELECT x FROM (VALUES 1, 3, 5) AS t(x) WHERE x BETWEEN SYMMETRIC 4 AND 2 AND x > 0;

SELECT '-- JSON (objects only)';
SELECT JSON '{"a": [1, 2, 3]}';
SELECT json_parse('{"a": 1}'), CAST(json_parse('{"a": 1}') AS VARCHAR);
SELECT json_format(json_parse('{"b": 2, "a": 1}'));
SELECT json_extract_scalar(json_parse('{"a": {"b": 3}}'), '$.a.b');
SELECT json_extract('{"a": {"b": [5, 6]}}', '$.a.b'), json_extract(JSON '{"a": {"b": [5, 6]}}', '$.a.b[0]');
SELECT json_size('{"a": {"b": [5, 6], "c": 1}}', '$.a');
SELECT json_array_contains('[1, 2, 3]', 2), json_array_contains('["x", "y"]', 'x'), json_array_contains('[true, false]', false);
SELECT is_json_scalar('1'), is_json_scalar('"abc"'), is_json_scalar('[1, 2]'), is_json_scalar('{"a": 1}');
SELECT json_query('{"a": [1, 2]}', '$.a'), json_exists('{"a": 1}', '$.a'), json_exists('{"a": 1}', '$.b');

SELECT '-- ClickHouse functions remain accessible';
SELECT toTypeName(1 = 1), arrayStringConcat(ARRAY['x', 'y'], '/');

SELECT '-- switching back';
SET dialect = 'clickhouse';
SELECT length('héllo');
