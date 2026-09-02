SET enable_analyzer = 1;

DROP TABLE IF EXISTS test_table;
CREATE TABLE test_table
(
    id UInt64,
    value String,
    INDEX value_idx (value) TYPE set(1000) GRANULARITY 1
) ENGINE=MergeTree ORDER BY id;

INSERT INTO test_table SELECT number, toString(number) FROM numbers(10);

SELECT count() FROM test_table WHERE value = '1' SETTINGS force_data_skipping_indices = 'value_idx';

SELECT count() FROM test_table AS t1 INNER JOIN (SELECT number AS id FROM numbers(10)) AS t2 ON t1.id = t2.id
WHERE t1.value = '1' SETTINGS force_data_skipping_indices = 'value_idx';

DROP TABLE test_table;
