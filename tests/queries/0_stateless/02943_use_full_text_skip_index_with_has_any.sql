DROP TABLE IF EXISTS tokenbf_v1_hasany_test;
DROP TABLE IF EXISTS ngrambf_v1_hasany_test;

CREATE TABLE tokenbf_v1_hasany_test
(
    id UInt32,
    array Array(String),
    INDEX idx_array_tokenbf_v1 array TYPE tokenbf_v1(512,3,0) GRANULARITY 1,
) Engine=MergeTree() ORDER BY id SETTINGS index_granularity = 1;

CREATE TABLE ngrambf_v1_hasany_test
(
    id UInt32,
    array Array(String),
    INDEX idx_array_ngrambf_v1 array TYPE ngrambf_v1(3,512,3,0) GRANULARITY 1,
) Engine=MergeTree() ORDER BY id SETTINGS index_granularity = 1;

INSERT INTO tokenbf_v1_hasany_test VALUES (1, ['this is a test', 'example.com']), (2, ['another test', 'another example']);
INSERT INTO ngrambf_v1_hasany_test VALUES (1, ['this is a test', 'example.com']), (2, ['another test', 'another example']);

SELECT * FROM tokenbf_v1_hasany_test WHERE hasAny(array, ['this is a test']) SETTINGS force_data_skipping_indices='idx_array_tokenbf_v1';
SELECT '--';
SELECT * FROM tokenbf_v1_hasany_test WHERE hasAny(array, ['example.com']) SETTINGS force_data_skipping_indices='idx_array_tokenbf_v1';
SELECT '--';
SELECT * FROM tokenbf_v1_hasany_test WHERE hasAny(array, ['another test']) SETTINGS force_data_skipping_indices='idx_array_tokenbf_v1';
SELECT '--';
SELECT * FROM tokenbf_v1_hasany_test WHERE hasAny(array, ['another example', 'example.com']) ORDER BY id ASC SETTINGS force_data_skipping_indices='idx_array_tokenbf_v1';
SELECT '--';

SELECT * FROM ngrambf_v1_hasany_test WHERE hasAny(array, ['this is a test']) SETTINGS force_data_skipping_indices='idx_array_ngrambf_v1';
SELECT '--';
SELECT * FROM ngrambf_v1_hasany_test WHERE hasAny(array, ['example.com']) SETTINGS force_data_skipping_indices='idx_array_ngrambf_v1';
SELECT '--';
SELECT * FROM ngrambf_v1_hasany_test WHERE hasAny(array, ['another test']) SETTINGS force_data_skipping_indices='idx_array_ngrambf_v1';
SELECT '--';
SELECT * FROM ngrambf_v1_hasany_test WHERE hasAny(array, ['another example', 'example.com']) ORDER BY id ASC SETTINGS force_data_skipping_indices='idx_array_ngrambf_v1';

DROP TABLE tokenbf_v1_hasany_test;
DROP TABLE ngrambf_v1_hasany_test;
