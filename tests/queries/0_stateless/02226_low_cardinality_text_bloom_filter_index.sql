DROP TABLE IF EXISTS bf_tokenbf_lowcard_test;
DROP TABLE IF EXISTS bf_ngram_lowcard_test;

CREATE TABLE bf_tokenbf_lowcard_test
(
    row_id UInt32,
    lc LowCardinality(String),
    lc_fixed LowCardinality(FixedString(8)),
    INDEX lc_bf_tokenbf lc TYPE tokenbf_v1(256,2,0) GRANULARITY 1,
    INDEX lc_fixed_bf_tokenbf lc_fixed TYPE tokenbf_v1(256,2,0) GRANULARITY 1
) Engine=MergeTree() ORDER BY row_id SETTINGS index_granularity = 1;

CREATE TABLE bf_ngram_lowcard_test
(
    row_id UInt32,
    lc LowCardinality(String),
    lc_fixed LowCardinality(FixedString(8)),
    INDEX lc_ngram lc TYPE ngrambf_v1(4,256,2,0) GRANULARITY 1,
    INDEX lc_fixed_ngram lc_fixed TYPE ngrambf_v1(4,256,2,0) GRANULARITY 1
) Engine=MergeTree() ORDER BY row_id SETTINGS index_granularity = 1;

INSERT INTO bf_tokenbf_lowcard_test VALUES (1, 'K1', 'K1ZZZZZZ'), (2, 'K2', 'K2ZZZZZZ');
INSERT INTO bf_ngram_lowcard_test VALUES (1, 'K1', 'K1ZZZZZZ'), (2, 'K2', 'K2ZZZZZZ');
INSERT INTO bf_tokenbf_lowcard_test VALUES (3, 'abCD3ef', 'abCD3ef'), (4, 'abCD4ef', 'abCD4ef');
INSERT INTO bf_ngram_lowcard_test   VALUES (3, 'abCD3ef', 'abCD3ef'), (4, 'abCD4ef', 'abCD4ef');

SELECT 'lc_bf_tokenbf';
SELECT * FROM bf_tokenbf_lowcard_test WHERE like(lc, 'K1') SETTINGS force_data_skipping_indices='lc_bf_tokenbf';
SELECT * FROM bf_tokenbf_lowcard_test WHERE like(lc, 'K2') SETTINGS force_data_skipping_indices='lc_bf_tokenbf';
SELECT * FROM bf_tokenbf_lowcard_test WHERE like(lc, 'K3') SETTINGS force_data_skipping_indices='lc_bf_tokenbf';

SELECT 'lc_fixed_bf_tokenbf';
SELECT * FROM bf_tokenbf_lowcard_test WHERE like(lc_fixed, 'K1ZZZZZZ') SETTINGS force_data_skipping_indices='lc_fixed_bf_tokenbf';
SELECT * FROM bf_tokenbf_lowcard_test WHERE like(lc_fixed, 'K2ZZZZZZ') SETTINGS force_data_skipping_indices='lc_fixed_bf_tokenbf';
SELECT * FROM bf_tokenbf_lowcard_test WHERE like(lc_fixed, 'K3ZZZZZZ') SETTINGS force_data_skipping_indices='lc_fixed_bf_tokenbf';

SELECT 'lc_ngram';
SELECT * FROM bf_ngram_lowcard_test WHERE like(lc, 'K1') SETTINGS force_data_skipping_indices='lc_ngram';
SELECT * FROM bf_ngram_lowcard_test WHERE like(lc, 'K2') SETTINGS force_data_skipping_indices='lc_ngram';
SELECT * FROM bf_ngram_lowcard_test WHERE like(lc, 'K3') SETTINGS force_data_skipping_indices='lc_ngram';

SELECT 'lc_fixed_ngram';
SELECT * FROM bf_ngram_lowcard_test WHERE like(lc_fixed, 'K1ZZZZZZ') SETTINGS force_data_skipping_indices='lc_fixed_ngram';
SELECT * FROM bf_ngram_lowcard_test WHERE like(lc_fixed, 'K2ZZZZZZ') SETTINGS force_data_skipping_indices='lc_fixed_ngram';
SELECT * FROM bf_ngram_lowcard_test WHERE like(lc_fixed, 'K3ZZZZZZ') SETTINGS force_data_skipping_indices='lc_fixed_ngram';


SELECT 'lc_bf_tokenbf';
SELECT * FROM bf_tokenbf_lowcard_test WHERE like(lc, '%CD3%') SETTINGS force_data_skipping_indices='lc_bf_tokenbf';
SELECT * FROM bf_tokenbf_lowcard_test WHERE like(lc, '%CD4%') SETTINGS force_data_skipping_indices='lc_bf_tokenbf';
SELECT * FROM bf_tokenbf_lowcard_test WHERE like(lc, '%CD5%') SETTINGS force_data_skipping_indices='lc_bf_tokenbf';

SELECT 'lc_fixed_bf_tokenbf';
SELECT * FROM bf_tokenbf_lowcard_test WHERE like(lc_fixed, '%CD3%') SETTINGS force_data_skipping_indices='lc_fixed_bf_tokenbf';
SELECT * FROM bf_tokenbf_lowcard_test WHERE like(lc_fixed, '%CD4%') SETTINGS force_data_skipping_indices='lc_fixed_bf_tokenbf';
SELECT * FROM bf_tokenbf_lowcard_test WHERE like(lc_fixed, '%CD5%') SETTINGS force_data_skipping_indices='lc_fixed_bf_tokenbf';

SELECT 'lc_ngram';
SELECT * FROM bf_ngram_lowcard_test WHERE like(lc, '%CD3%') SETTINGS force_data_skipping_indices='lc_ngram';
SELECT * FROM bf_ngram_lowcard_test WHERE like(lc, '%CD4%') SETTINGS force_data_skipping_indices='lc_ngram';
SELECT * FROM bf_ngram_lowcard_test WHERE like(lc, '%CD5%') SETTINGS force_data_skipping_indices='lc_ngram';

SELECT 'lc_fixed_ngram';
SELECT * FROM bf_ngram_lowcard_test WHERE like(lc_fixed, '%CD3%') SETTINGS force_data_skipping_indices='lc_fixed_ngram';
SELECT * FROM bf_ngram_lowcard_test WHERE like(lc_fixed, '%CD4%') SETTINGS force_data_skipping_indices='lc_fixed_ngram';
SELECT * FROM bf_ngram_lowcard_test WHERE like(lc_fixed, '%CD5%') SETTINGS force_data_skipping_indices='lc_fixed_ngram';

DROP TABLE bf_tokenbf_lowcard_test;
DROP TABLE bf_ngram_lowcard_test;
