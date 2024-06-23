-- Tags: no-parallel, no-replicated-database: Named collection is used

SET allow_experimental_object_type = 1;
--

DROP NAMED COLLECTION IF EXISTS 02918_json_fuzzer;
CREATE NAMED COLLECTION 02918_json_fuzzer AS json_str='{}';

SELECT * FROM fuzzJSON(02918_json_fuzzer, random_seed=54321) LIMIT 10;
SELECT * FROM fuzzJSON(02918_json_fuzzer, json_str='{"ClickHouse":"Is Fast"}', random_seed=1337) LIMIT 20;
SELECT * FROM fuzzJSON(02918_json_fuzzer, json_str='{"students":[{"name":"Alice"}, {"name":"Bob"}]}', random_seed=1337) LIMIT 20;
SELECT * FROM fuzzJSON(02918_json_fuzzer, json_str='{"schedule":[{"breakfast":"7am"}, {"lunch":"12pm"}]}', random_seed=123456, reuse_output=true) LIMIT 20;
SELECT * FROM fuzzJSON(02918_json_fuzzer, json_str='{"schedule":[{"breakfast":"7am"}, {"lunch":"12pm"}]}', random_seed=123456, reuse_output=false) LIMIT 20;
SELECT * FROM fuzzJSON(02918_json_fuzzer,
    json_str='{"schedule":[{"breakfast":"7am"}, {"lunch":"12pm"}]}',
    random_seed=123456,
    reuse_output=0,
    max_output_length=128) LIMIT 20;

SELECT * FROM fuzzJSON(02918_json_fuzzer,
    json_str='{"schedule":[{"breakfast":"7am"}, {"lunch":"12pm"}]}',
    random_seed=123456,
    reuse_output=0,
    max_output_length=65536,
    max_nesting_level=10,
    max_array_size=20) LIMIT 20;

SELECT * FROM fuzzJSON(02918_json_fuzzer,
    random_seed=6667,
    max_nesting_level=0) LIMIT 10;

SELECT * FROM fuzzJSON(02918_json_fuzzer,
    random_seed=6667,
    max_object_size=0,
    max_array_size=0) LIMIT 10;

--
DROP TABLE IF EXISTS 02918_table_str;
CREATE TABLE 02918_table_str (json_str String) Engine=Memory;

INSERT INTO 02918_table_str SELECT * FROM fuzzJSON(02918_json_fuzzer) limit 10;
INSERT INTO 02918_table_str SELECT * FROM fuzzJSON(02918_json_fuzzer) limit 10;
INSERT INTO 02918_table_str SELECT * FROM fuzzJSON(02918_json_fuzzer, random_seed=123, reuse_output=true) limit 10;
INSERT INTO 02918_table_str SELECT * FROM fuzzJSON(
    02918_json_fuzzer,
    json_str='{"name": "John Doe", "age": 30, "address": {"city": "Citiville", "zip": "12345"}, "hobbies": ["reading", "traveling", "coding"]}',
    random_seed=6666) LIMIT 200;

INSERT INTO 02918_table_str SELECT * FROM fuzzJSON(
    02918_json_fuzzer,
    json_str='{"name": "John Doe", "age": 30, "address": {"city": "Citiville", "zip": "12345"}, "hobbies": ["reading", "traveling", "coding"]}',
    random_seed=6666,
    min_key_length=1,
    max_key_length=5) LIMIT 200;

INSERT INTO 02918_table_str SELECT * FROM fuzzJSON(
    02918_json_fuzzer,
    json_str='{"name": "John Doe", "age": 30, "address": {"city": "Citiville", "zip": "12345"}, "hobbies": ["reading", "traveling", "coding"]}',
    max_nesting_level=128,
    reuse_output=true,
    random_seed=6666,
    min_key_length=5,
    max_key_length=5) LIMIT 200;

INSERT INTO 02918_table_str SELECT * FROM fuzzJSON(
    02918_json_fuzzer,
    json_str='{"name": "John Doe", "age": 30, "address": {"city": "Citiville", "zip": "12345"}, "hobbies": ["reading", "traveling", "coding"]}',
    random_seed=6666,
    reuse_output=1,
    probability=0.5,
    max_output_length=65536,
    max_nesting_level=18446744073709551615,
    max_array_size=18446744073709551615,
    max_object_size=18446744073709551615,
    max_key_length=65536,
    max_string_value_length=65536) LIMIT 100;

SELECT count() FROM 02918_table_str;

DROP TABLE IF EXISTS 02918_table_str;

--
SELECT * FROM fuzzJSON(02918_json_fuzzer, max_output_length="Hello") LIMIT 10; -- { serverError BAD_ARGUMENTS }
SELECT * FROM fuzzJSON(02918_json_fuzzer, max_output_length=65537) LIMIT 10; -- { serverError BAD_ARGUMENTS }
SELECT * FROM fuzzJSON(02918_json_fuzzer, probability=10) LIMIT 10; -- { serverError BAD_ARGUMENTS }
SELECT * FROM fuzzJSON(02918_json_fuzzer, probability=-0.1) LIMIT 10; -- { serverError BAD_ARGUMENTS }
SELECT * FROM fuzzJSON(02918_json_fuzzer, probability=1.1) LIMIT 10; -- { serverError BAD_ARGUMENTS }
SELECT * FROM fuzzJSON(02918_json_fuzzer, probability=1.1) LIMIT 10; -- { serverError BAD_ARGUMENTS }
SELECT * FROM fuzzJSON(02918_json_fuzzer, max_string_value_length=65537) LIMIT 10; -- { serverError BAD_ARGUMENTS }
SELECT * FROM fuzzJSON(02918_json_fuzzer, max_key_length=65537) LIMIT 10; -- { serverError BAD_ARGUMENTS }
SELECT * FROM fuzzJSON(02918_json_fuzzer, max_key_length=10, min_key_length=0) LIMIT 10; -- { serverError BAD_ARGUMENTS }
SELECT * FROM fuzzJSON(02918_json_fuzzer, max_key_length=10, min_key_length=11) LIMIT 10; -- { serverError BAD_ARGUMENTS }

--
DROP TABLE IF EXISTS 02918_table_obj1;
CREATE TABLE 02918_table_obj1 (json_obj Object('json')) Engine=Memory;

INSERT INTO 02918_table_obj1 SELECT * FROM fuzzJSON(
    02918_json_fuzzer,
    json_str='{"name": "John Doe", "age": 27, "address": {"city": "Citiville", "zip": "12345"}, "hobbies": ["reading", "traveling", "coding"]}',
    random_seed=12345) LIMIT 200;

SELECT count() FROM 02918_table_obj1;

DROP TABLE IF EXISTS 02918_table_obj1;

--
DROP TABLE IF EXISTS 02918_table_obj2;
CREATE TABLE 02918_table_obj2 (json_obj Object('json')) Engine=Memory;

INSERT INTO 02918_table_obj2 SELECT * FROM fuzzJSON(
    02918_json_fuzzer,
    json_str=
    '{
      "name": {
        "first": "Joan",
        "last": "of Arc"
      },
      "birth": {"date": "January 6, 1412", "place": "Domremy, France"},
      "death": {"date": "May 30, 1431", "place": "Rouen, France"},
      "occupation": "Military Leader",
      "achievements": ["Lifted Siege of Orleans", "Assisted in Charles VII\'s Coronation"],
      "legacy": {
        "honors": ["Canonized Saint", "National Heroine of France"],
        "memorials": [
        {"name": "Joan of Arc Memorial", "location": "Domremy"},
        {"name": "Place Jeanne d\'Arc", "location": "Rouen"}
        ]
      }
    }',
    random_seed=12345,
    max_output_length=1024) LIMIT 50;

INSERT INTO 02918_table_obj2 SELECT * FROM fuzzJSON(
    02918_json_fuzzer,
    json_str=
    '{
      "name": {
        "first": "Joan",
        "last": "of Arc"
      },
      "birth": {"date": "January 6, 1412", "place": "Domremy, France"},
      "death": {"date": "May 30, 1431", "place": "Rouen, France"},
      "occupation": "Military Leader",
      "achievements": ["Lifted Siege of Orleans", "Assisted in Charles VII\'s Coronation"],
      "legacy": {
        "honors": ["Canonized Saint", "National Heroine of France"],
        "memorials": [
        {"name": "Joan of Arc Memorial", "location": "Domremy"},
        {"name": "Place Jeanne d\'Arc", "location": "Rouen"}
        ]
      }
    }',
    random_seed=12345,
    max_output_length=1024, malform_output=true) LIMIT 50; -- {serverError INCORRECT_DATA }

SELECT count() FROM 02918_table_obj2;

DROP TABLE IF EXISTS 02918_table_obj2;

DROP NAMED COLLECTION IF EXISTS 02918_json_fuzzer;
