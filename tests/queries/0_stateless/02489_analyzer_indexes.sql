SET enable_analyzer = 1;

DROP TABLE IF EXISTS test_table;
CREATE TABLE test_table
(
    id UInt64,
    value_1 String,
    value_2 String,
    value_3 String,
    INDEX value_1_idx (value_1) TYPE bloom_filter GRANULARITY 1,
    INDEX value_2_idx (value_2) TYPE ngrambf_v1(3, 512, 2, 0) GRANULARITY 1,
    INDEX value_3_idx (value_3) TYPE tokenbf_v1(512, 3, 0) GRANULARITY 1
) ENGINE=MergeTree ORDER BY id;

INSERT INTO test_table SELECT number, toString(number), toString(number), toString(number) FROM numbers(10);

SELECT count() FROM test_table WHERE id = 1 SETTINGS force_primary_key = 1;

SELECT count() FROM test_table WHERE value_1 = '1' SETTINGS force_data_skipping_indices = 'value_1_idx';

SELECT count() FROM test_table WHERE id = 1 AND value_1 = '1' SETTINGS force_primary_key = 1, force_data_skipping_indices = 'value_1_idx';

SELECT count() FROM test_table WHERE value_2 = '1' SETTINGS force_data_skipping_indices = 'value_2_idx';

SELECT count() FROM test_table WHERE value_1 = '1' AND value_2 = '1' SETTINGS force_data_skipping_indices = 'value_1_idx, value_2_idx';

SELECT count() FROM test_table WHERE id = 1 AND value_1 = '1' AND value_2 = '1' SETTINGS force_primary_key = 1, force_data_skipping_indices = 'value_1_idx, value_2_idx';

SELECT count() FROM test_table WHERE value_3 = '1' SETTINGS force_data_skipping_indices = 'value_3_idx';

SELECT count() FROM test_table WHERE id = 1 AND value_3 = '1' SETTINGS force_primary_key = 1, force_data_skipping_indices = 'value_3_idx';

SELECT count() FROM test_table WHERE id = 1 AND value_1 = '1' AND value_2 = '1' AND value_3 = '1'
SETTINGS force_primary_key = 1, force_data_skipping_indices = 'value_1_idx, value_2_idx, value_3_idx';

SELECT count() FROM test_table AS t1 INNER JOIN (SELECT number AS id FROM numbers(10)) AS t2 ON t1.id = t2.id
WHERE t1.id = 1 SETTINGS force_primary_key = 1;

SELECT count() FROM test_table AS t1 INNER JOIN (SELECT number AS id FROM numbers(10)) AS t2 ON t1.id = t2.id
WHERE t1.value_1 = '1' SETTINGS force_data_skipping_indices = 'value_1_idx';

SELECT count() FROM test_table AS t1 INNER JOIN (SELECT number AS id FROM numbers(10)) AS t2 ON t1.id = t2.id
WHERE t1.id = 1 AND t1.value_1 = '1' SETTINGS force_primary_key = 1, force_data_skipping_indices = 'value_1_idx';

SELECT count() FROM test_table AS t1 INNER JOIN (SELECT number AS id FROM numbers(10)) AS t2 ON t1.id = t2.id
WHERE t1.value_2 = '1' SETTINGS force_data_skipping_indices = 'value_2_idx';

SELECT count() FROM test_table AS t1 INNER JOIN (SELECT number AS id FROM numbers(10)) AS t2 ON t1.id = t2.id
WHERE t1.value_1 = '1' AND t1.value_2 = '1' SETTINGS force_data_skipping_indices = 'value_1_idx, value_2_idx';

SELECT count() FROM test_table AS t1 INNER JOIN (SELECT number AS id FROM numbers(10)) AS t2 ON t1.id = t2.id
WHERE t1.id = 1 AND t1.value_1 = '1' AND t1.value_2 = '1' SETTINGS force_primary_key = 1, force_data_skipping_indices = 'value_1_idx, value_2_idx';

SELECT count() FROM test_table AS t1 INNER JOIN (SELECT number AS id FROM numbers(10)) AS t2 ON t1.id = t2.id
WHERE t1.value_3 = '1' SETTINGS force_data_skipping_indices = 'value_3_idx';

SELECT count() FROM test_table AS t1 INNER JOIN (SELECT number AS id FROM numbers(10)) AS t2 ON t1.id = t2.id
WHERE t1.id = 1 AND t1.value_1 = '1' AND t1.value_2 = '1' AND t1.value_3 = '1'
SETTINGS force_primary_key = 1, force_data_skipping_indices = 'value_1_idx, value_2_idx, value_3_idx';

DROP TABLE test_table;
