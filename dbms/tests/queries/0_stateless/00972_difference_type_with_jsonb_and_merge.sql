DROP TABLE IF EXISTS test.merge_with_difference_type;

CREATE TABLE test.merge_with_difference_type(day Date, id UInt64, test JSONB) ENGINE = MergeTree PARTITION BY day ORDER BY (id, day);

INSERT INTO test.merge_with_difference_type VALUES('2019-01-01', 1, 'true');
INSERT INTO test.merge_with_difference_type VALUES('2019-01-01', 1, '2147483648'), ('2019-01-01', 3, '4294967296');
INSERT INTO test.merge_with_difference_type VALUES('2019-01-01', 3, '"test_string_data"');
INSERT INTO test.merge_with_difference_type VALUES('2019-01-01', 4, '{"uid": 123456, "view_url": "http://yandex.ru", "is_first_view": true}');
INSERT INTO test.merge_with_difference_type VALUES('2019-01-01', 4, '{"uid": 123456, "view_url": "http://yandex.ru", "is_first_view": true, "session_info": {"cookies": "abcdefg", "country_code": 1, "is_male": true}}');

OPTIMIZE TABLE test.merge_with_difference_type FINAL;

SELECT * FROM test.merge_with_difference_type;

DROP TABLE IF EXISTS test.merge_with_difference_type;

