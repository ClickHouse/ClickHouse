-- Tags: no-fasttest

SET allow_experimental_json_type = 1;

DROP TABLE IF EXISTS test_vertical_json;

CREATE TABLE test_vertical_json
(
    id UInt32,
    data JSON
)
ENGINE = Memory;

INSERT INTO test_vertical_json VALUES (1, '{"name": "Alice", "age": 30, "address": {"city": "New York", "zip": "10001"}, "hobbies": ["reading", "cycling"]}');
INSERT INTO test_vertical_json VALUES (2, '{"name": "Bob", "age": 25, "address": {"city": "Los Angeles", "zip": "90001"}, "hobbies": ["gaming", "cooking", "traveling"]}');
INSERT INTO test_vertical_json VALUES (3, NULL);

SELECT * FROM test_vertical_json ORDER BY id FORMAT Vertical;

DROP TABLE test_vertical_json;

