DROP TABLE IF EXISTS test_data;

CREATE TABLE test_data (
    id UInt32,
    s String,
    s_nullable Nullable(String),
    s_empty String,
    test_uint64 UInt64,
    username String,
    article String,
    upvotes UInt32,
    name String,
    description String,
    title String,
    content String,
    str_array Array(String),
    int_array Array(UInt64),
    nested_array Array(Array(String)),
    empty_array Array(String),
    string_map Map(String, String),
    int_map Map(String, UInt64),
    empty_map Map(String, String)
) ENGINE = Memory;

INSERT INTO test_data SELECT
    number AS id,
    if(number % 3 = 0, '', 'test') AS s,
    if(number % 4 = 0, NULL, 'value') AS s_nullable,
    '' AS s_empty,
    number * 100 AS test_uint64,
    'user_' || toString(number) AS username,
    'Article ' || toString(number) AS article,
    number % 100 AS upvotes,
    if(number % 5 = 0, '', 'item' || toString(number)) AS name,
    if(number % 3 = 0, '', 'description' || toString(number)) AS description,
    if(number % 6 = 0, '', 'title' || toString(number)) AS title,
    if(number % 4 = 0, '', 'content' || toString(number)) AS content,
    if(number % 3 = 0, ['a'], ['a', 'b']) AS str_array,
    if(number % 5 = 0, [], [1, 2]) AS int_array,
    if(number % 3 = 0, [[]], [['a'], ['b']]) AS nested_array,
    [] AS empty_array,
    if(number % 5 = 0, map(), map('key', 'value')) AS string_map,
    if(number % 5 = 0, map(), map('num', 100)) AS int_map,
    map() AS empty_map
FROM system.numbers LIMIT 100;

SELECT 'Test 1: Basic length function';
(SELECT length(s), length('constant'), test_uint64 FROM test_data WHERE s != '' ORDER BY id SETTINGS query_plan_push_down_volume_reducing_functions = 0)
EXCEPT
(SELECT length(s), length('constant'), test_uint64 FROM test_data WHERE s != '' ORDER BY id SETTINGS query_plan_push_down_volume_reducing_functions = 1);

SELECT 'Test 2: notEmpty function';
(SELECT notEmpty(s), notEmpty(s_nullable), id FROM test_data ORDER BY id SETTINGS query_plan_push_down_volume_reducing_functions = 0)
EXCEPT
(SELECT notEmpty(s), notEmpty(s_nullable), id FROM test_data ORDER BY id SETTINGS query_plan_push_down_volume_reducing_functions = 1);

SELECT 'Test 3: empty function';
(SELECT empty(s), empty(s_empty), test_uint64 FROM test_data WHERE test_uint64 > 200 ORDER BY id DESC SETTINGS query_plan_push_down_volume_reducing_functions = 0)
EXCEPT
(SELECT empty(s), empty(s_empty), test_uint64 FROM test_data WHERE test_uint64 > 200 ORDER BY id DESC SETTINGS query_plan_push_down_volume_reducing_functions = 1);

SELECT 'Test 4: Multiple volume reducing functions';
(SELECT length(username), length(article), notEmpty(username), empty(username) FROM test_data WHERE id <= 10 ORDER BY upvotes DESC SETTINGS query_plan_push_down_volume_reducing_functions = 0)
EXCEPT
(SELECT length(username), length(article), notEmpty(username), empty(username) FROM test_data WHERE id <= 10 ORDER BY upvotes DESC SETTINGS query_plan_push_down_volume_reducing_functions = 1);

SELECT 'Test 5: With LIMIT';
(SELECT length(article), upvotes FROM test_data ORDER BY id LIMIT 5 SETTINGS query_plan_push_down_volume_reducing_functions = 0)
EXCEPT
(SELECT length(article), upvotes FROM test_data ORDER BY id LIMIT 5 SETTINGS query_plan_push_down_volume_reducing_functions = 1);

SELECT 'Test 6: With GROUP BY';
(SELECT length(username) as name_len, count(*) as cnt FROM test_data WHERE id <= 20 GROUP BY length(username) ORDER BY name_len SETTINGS query_plan_push_down_volume_reducing_functions = 0)
EXCEPT
(SELECT length(username) as name_len, count(*) as cnt FROM test_data WHERE id <= 20 GROUP BY length(username) ORDER BY name_len SETTINGS query_plan_push_down_volume_reducing_functions = 1);

SELECT 'Test 7: With JOINs';
(SELECT length(l.name), length(r.title), l.id FROM test_data l INNER JOIN test_data r ON l.id = r.id WHERE notEmpty(l.name) ORDER BY l.id SETTINGS query_plan_push_down_volume_reducing_functions = 0)
EXCEPT
(SELECT length(l.name), length(r.title), l.id FROM test_data l INNER JOIN test_data r ON l.id = r.id WHERE notEmpty(l.name) ORDER BY l.id SETTINGS query_plan_push_down_volume_reducing_functions = 1);

SELECT 'Test 8: With subqueries';
(SELECT length(username), id FROM test_data WHERE id IN (SELECT id FROM test_data WHERE length(article) > 20) ORDER BY id LIMIT 5 SETTINGS query_plan_push_down_volume_reducing_functions = 0)
EXCEPT
(SELECT length(username), id FROM test_data WHERE id IN (SELECT id FROM test_data WHERE length(article) > 20) ORDER BY id LIMIT 5 SETTINGS query_plan_push_down_volume_reducing_functions = 1);

SELECT 'Test 9: Complex WHERE conditions';
(SELECT length(s), notEmpty(s_nullable), empty(s_empty) FROM test_data WHERE length(s) > 3 AND notEmpty(s) AND test_uint64 BETWEEN 200 AND 400 ORDER BY length(s) DESC SETTINGS query_plan_push_down_volume_reducing_functions = 0)
EXCEPT
(SELECT length(s), notEmpty(s_nullable), empty(s_empty) FROM test_data WHERE length(s) > 3 AND notEmpty(s) AND test_uint64 BETWEEN 200 AND 400 ORDER BY length(s) DESC SETTINGS query_plan_push_down_volume_reducing_functions = 1);

SELECT 'Test 10: Nested functions';
(SELECT length(concat(s, '_suffix')), length(upper(s)) FROM test_data WHERE s != '' ORDER BY id SETTINGS query_plan_push_down_volume_reducing_functions = 0)
EXCEPT
(SELECT length(concat(s, '_suffix')), length(upper(s)) FROM test_data WHERE s != '' ORDER BY id SETTINGS query_plan_push_down_volume_reducing_functions = 1);

SELECT 'Test 11: UNION ALL';
(SELECT length(s) as len FROM test_data WHERE id <= 2 UNION ALL SELECT length(username) as len FROM test_data WHERE id <= 3 ORDER BY len SETTINGS query_plan_push_down_volume_reducing_functions = 0)
EXCEPT
(SELECT length(s) as len FROM test_data WHERE id <= 2 UNION ALL SELECT length(username) as len FROM test_data WHERE id <= 3 ORDER BY len SETTINGS query_plan_push_down_volume_reducing_functions = 1);

SELECT 'Test 12: Array length functions';
(SELECT length(str_array), length(int_array), length(nested_array), id FROM test_data WHERE length(str_array) > 0 ORDER BY id SETTINGS query_plan_push_down_volume_reducing_functions = 0)
EXCEPT
(SELECT length(str_array), length(int_array), length(nested_array), id FROM test_data WHERE length(str_array) > 0 ORDER BY id SETTINGS query_plan_push_down_volume_reducing_functions = 1);

SELECT 'Test 13: Array empty/notEmpty functions';
(SELECT notEmpty(str_array), empty(empty_array), notEmpty(int_array), id FROM test_data ORDER BY id SETTINGS query_plan_push_down_volume_reducing_functions = 0)
EXCEPT
(SELECT notEmpty(str_array), empty(empty_array), notEmpty(int_array), id FROM test_data ORDER BY id SETTINGS query_plan_push_down_volume_reducing_functions = 1);

SELECT 'Test 14: Map length and empty functions';
(SELECT length(string_map), length(int_map), empty(empty_map), id FROM test_data ORDER BY id SETTINGS query_plan_push_down_volume_reducing_functions = 0)
EXCEPT
(SELECT length(string_map), length(int_map), empty(empty_map), id FROM test_data ORDER BY id SETTINGS query_plan_push_down_volume_reducing_functions = 1);

SELECT 'Test 15: ARRAY JOIN with volume reducing functions';
(SELECT length(str_element), length(str_array), id FROM test_data ARRAY JOIN str_array AS str_element WHERE notEmpty(str_element) ORDER BY id, str_element SETTINGS query_plan_push_down_volume_reducing_functions = 0)
EXCEPT
(SELECT length(str_element), length(str_array), id FROM test_data ARRAY JOIN str_array AS str_element WHERE notEmpty(str_element) ORDER BY id, str_element SETTINGS query_plan_push_down_volume_reducing_functions = 1);

SELECT 'Test 16: Array functions in WHERE with complex conditions';
(SELECT length(str_array), length(int_array), id FROM test_data WHERE length(str_array) > 1 AND notEmpty(int_array) AND length(nested_array) >= 1 ORDER BY length(str_array) DESC, id SETTINGS query_plan_push_down_volume_reducing_functions = 0)
EXCEPT
(SELECT length(str_array), length(int_array), id FROM test_data WHERE length(str_array) > 1 AND notEmpty(int_array) AND length(nested_array) >= 1 ORDER BY length(str_array) DESC, id SETTINGS query_plan_push_down_volume_reducing_functions = 1);

SELECT 'Test 17: Map functions in aggregations';
(SELECT length(string_map) as map_size, count(*) as cnt FROM test_data WHERE notEmpty(string_map) GROUP BY length(string_map) ORDER BY map_size SETTINGS query_plan_push_down_volume_reducing_functions = 0)
EXCEPT
(SELECT length(string_map) as map_size, count(*) as cnt FROM test_data WHERE notEmpty(string_map) GROUP BY length(string_map) ORDER BY map_size SETTINGS query_plan_push_down_volume_reducing_functions = 1);

SELECT 'Test 18: Combined array and map operations';
(SELECT a.id, length(a.str_array) as arr_len, length(m.string_map) as map_len FROM test_data a JOIN test_data m ON a.id = m.id WHERE length(a.str_array) + length(m.string_map) > 0 ORDER BY a.id SETTINGS query_plan_push_down_volume_reducing_functions = 0)
EXCEPT
(SELECT a.id, length(a.str_array) as arr_len, length(m.string_map) as map_len FROM test_data a JOIN test_data m ON a.id = m.id WHERE length(a.str_array) + length(m.string_map) > 0 ORDER BY a.id SETTINGS query_plan_push_down_volume_reducing_functions = 1);

DROP TABLE test_data;
