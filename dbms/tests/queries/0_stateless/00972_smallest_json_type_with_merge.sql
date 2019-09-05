DROP TABLE IF EXISTS test.merge_with_difference_type;

CREATE TABLE test.merge_with_difference_type(day Date, id UInt64, test SmallestJSON) ENGINE = MergeTree PARTITION BY day ORDER BY (id, day);

INSERT INTO test.merge_with_difference_type VALUES('2019-01-01', 1, 'true');
INSERT INTO test.merge_with_difference_type VALUES('2019-01-01', 1, '2147483648'), ('2019-01-01', 3, '4294967296');
INSERT INTO test.merge_with_difference_type VALUES('2019-01-01', 3, '"test_string_data"');

OPTIMIZE TABLE test.merge_with_difference_type FINAL;

SELECT * FROM test.merge_with_difference_type;

DROP TABLE IF EXISTS test.merge_with_difference_type;
