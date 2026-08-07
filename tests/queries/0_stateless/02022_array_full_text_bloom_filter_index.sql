DROP TABLE IF EXISTS bf_tokenbf_array_test;
DROP TABLE IF EXISTS bf_ngram_array_test;

CREATE TABLE bf_tokenbf_array_test
(
    row_id UInt32,
    array Array(String),
    array_fixed Array(FixedString(2)),
    INDEX array_bf_tokenbf array TYPE tokenbf_v1(256,2,0) GRANULARITY 1,
    INDEX array_fixed_bf_tokenbf array_fixed TYPE tokenbf_v1(256,2,0) GRANULARITY 1
) Engine=MergeTree() ORDER BY row_id SETTINGS index_granularity = 1;

CREATE TABLE bf_ngram_array_test
(
    row_id UInt32,
    array Array(String),
    array_fixed Array(FixedString(2)),
    INDEX array_ngram array TYPE ngrambf_v1(4,256,2,0) GRANULARITY 1,
    INDEX array_fixed_ngram array_fixed TYPE ngrambf_v1(4,256,2,0) GRANULARITY 1
) Engine=MergeTree() ORDER BY row_id SETTINGS index_granularity = 1;

INSERT INTO bf_tokenbf_array_test VALUES (1, ['K1'], ['K1']), (2, ['K2'], ['K2']);
INSERT INTO bf_ngram_array_test VALUES (1, ['K1'], ['K1']), (2, ['K2'], ['K2']);

SELECT * FROM bf_tokenbf_array_test WHERE has(array, 'K1') SETTINGS force_data_skipping_indices='array_bf_tokenbf';
SELECT * FROM bf_tokenbf_array_test WHERE has(array, 'K2') SETTINGS force_data_skipping_indices='array_bf_tokenbf';
SELECT * FROM bf_tokenbf_array_test WHERE has(array, 'K3') SETTINGS force_data_skipping_indices='array_bf_tokenbf';

SELECT * FROM bf_tokenbf_array_test WHERE has(array_fixed, 'K1') SETTINGS force_data_skipping_indices='array_fixed_bf_tokenbf';
SELECT * FROM bf_tokenbf_array_test WHERE has(array_fixed, 'K2') SETTINGS force_data_skipping_indices='array_fixed_bf_tokenbf';
SELECT * FROM bf_tokenbf_array_test WHERE has(array_fixed, 'K3') SETTINGS force_data_skipping_indices='array_fixed_bf_tokenbf';

SELECT * FROM bf_ngram_array_test WHERE has(array, 'K1') SETTINGS force_data_skipping_indices='array_ngram';
SELECT * FROM bf_ngram_array_test WHERE has(array, 'K2') SETTINGS force_data_skipping_indices='array_ngram';
SELECT * FROM bf_ngram_array_test WHERE has(array, 'K3') SETTINGS force_data_skipping_indices='array_ngram';

SELECT * FROM bf_ngram_array_test WHERE has(array_fixed, 'K1') SETTINGS force_data_skipping_indices='array_fixed_ngram';
SELECT * FROM bf_ngram_array_test WHERE has(array_fixed, 'K2') SETTINGS force_data_skipping_indices='array_fixed_ngram';
SELECT * FROM bf_ngram_array_test WHERE has(array_fixed, 'K3') SETTINGS force_data_skipping_indices='array_fixed_ngram';

DROP TABLE bf_tokenbf_array_test;
DROP TABLE bf_ngram_array_test;
