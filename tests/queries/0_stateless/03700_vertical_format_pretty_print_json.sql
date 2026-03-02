-- Tags: no-fasttest

DROP TABLE IF EXISTS test_vertical_json;

CREATE TABLE test_vertical_json
(
    id UInt32,
    data JSON,
    nullableData Nullable(JSON)
)
ENGINE = Memory;

INSERT INTO test_vertical_json VALUES (1, '{"name": "Alice", "age": 30, "address": {"city": "New York", "zip": "10001"}, "hobbies": ["reading", "cycling"]}', '{"foo": "bar"}');
INSERT INTO test_vertical_json VALUES (2, NULL, NULL);

SELECT * FROM test_vertical_json ORDER BY id FORMAT Vertical;

DROP TABLE test_vertical_json;

