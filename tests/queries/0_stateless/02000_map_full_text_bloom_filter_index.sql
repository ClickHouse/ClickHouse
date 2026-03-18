DROP TABLE IF EXISTS bf_tokenbf_map_keys_test;
DROP TABLE IF EXISTS bf_ngrambf_map_keys_test;

CREATE TABLE bf_tokenbf_map_keys_test
(
    row_id UInt32,
    map Map(String, String),
    map_fixed Map(FixedString(2), String),
    INDEX map_keys_tokenbf mapKeys(map) TYPE tokenbf_v1(256,2,0) GRANULARITY 1,
    INDEX map_fixed_keys_tokenbf mapKeys(map_fixed) TYPE ngrambf_v1(4,256,2,0) GRANULARITY 1
) Engine=MergeTree() ORDER BY row_id SETTINGS index_granularity = 1;

INSERT INTO bf_tokenbf_map_keys_test VALUES (0, {'K0':'V0'}, {'K0':'V0'}), (1, {'K1':'V1'}, {'K1':'V1'});

SELECT 'Map full text bloom filter tokenbf mapKeys';

SELECT 'Equals with existing key';
SELECT * FROM bf_tokenbf_map_keys_test WHERE map['K0'] = 'V0' SETTINGS force_data_skipping_indices='map_keys_tokenbf';
SELECT 'Equals with non existing key';
SELECT * FROM bf_tokenbf_map_keys_test WHERE map['K2'] = 'V2' SETTINGS force_data_skipping_indices='map_keys_tokenbf';
SELECT 'Equals with non existing key and default value';
SELECT * FROM bf_tokenbf_map_keys_test WHERE map['K3'] = '';
SELECT 'Not equals with existing key';
SELECT * FROM bf_tokenbf_map_keys_test WHERE map['K0'] != 'V0' SETTINGS force_data_skipping_indices='map_keys_tokenbf';
SELECT 'Not equals with non existing key';
SELECT * FROM bf_tokenbf_map_keys_test WHERE map['K2'] != 'V2' SETTINGS force_data_skipping_indices='map_keys_tokenbf';
SELECT 'Not equals with non existing key and default value';
SELECT * FROM bf_tokenbf_map_keys_test WHERE map['K3'] != '';

SELECT 'Map fixed full text bloom filter tokenbf mapKeys';

SELECT 'Equals with existing key';
SELECT * FROM bf_tokenbf_map_keys_test WHERE map_fixed['K0'] = 'V0' SETTINGS force_data_skipping_indices='map_fixed_keys_tokenbf';
SELECT 'Equals with non existing key';
SELECT * FROM bf_tokenbf_map_keys_test WHERE map_fixed['K2'] = 'V2' SETTINGS force_data_skipping_indices='map_fixed_keys_tokenbf';
SELECT 'Equals with non existing key and default value';
SELECT * FROM bf_tokenbf_map_keys_test WHERE map_fixed['K3'] = '';
SELECT 'Not equals with existing key';
SELECT * FROM bf_tokenbf_map_keys_test WHERE map_fixed['K0'] != 'V0' SETTINGS force_data_skipping_indices='map_fixed_keys_tokenbf';
SELECT 'Not equals with non existing key';
SELECT * FROM bf_tokenbf_map_keys_test WHERE map_fixed['K2'] != 'V2' SETTINGS force_data_skipping_indices='map_fixed_keys_tokenbf';
SELECT 'Not equals with non existing key and default value';
SELECT * FROM bf_tokenbf_map_keys_test WHERE map_fixed['K3'] != '';

DROP TABLE bf_tokenbf_map_keys_test;

CREATE TABLE bf_tokenbf_map_values_test
(
    row_id UInt32,
    map Map(String, String),
    map_fixed Map(FixedString(2), String),
    INDEX map_values_tokenbf mapValues(map) TYPE tokenbf_v1(256,2,0) GRANULARITY 1,
    INDEX map_fixed_values_tokenbf mapValues(map_fixed) TYPE ngrambf_v1(4,256,2,0) GRANULARITY 1
) Engine=MergeTree() ORDER BY row_id SETTINGS index_granularity = 1;

INSERT INTO bf_tokenbf_map_values_test VALUES (0, {'K0':'V0'}, {'K0':'V0'}), (1, {'K1':'V1'}, {'K1':'V1'});

SELECT 'Map full text bloom filter tokenbf mapValues';

SELECT 'Equals with existing key';
SELECT * FROM bf_tokenbf_map_values_test WHERE map['K0'] = 'V0' SETTINGS force_data_skipping_indices='map_values_tokenbf';
SELECT 'Equals with non existing key';
SELECT * FROM bf_tokenbf_map_values_test WHERE map['K2'] = 'V2' SETTINGS force_data_skipping_indices='map_values_tokenbf';
SELECT 'Equals with non existing key and default value';
SELECT * FROM bf_tokenbf_map_values_test WHERE map['K3'] = '';
SELECT 'Not equals with existing key';
SELECT * FROM bf_tokenbf_map_values_test WHERE map['K0'] != 'V0' SETTINGS force_data_skipping_indices='map_values_tokenbf';
SELECT 'Not equals with non existing key';
SELECT * FROM bf_tokenbf_map_values_test WHERE map['K2'] != 'V2' SETTINGS force_data_skipping_indices='map_values_tokenbf';
SELECT 'Not equals with non existing key and default value';
SELECT * FROM bf_tokenbf_map_values_test WHERE map['K3'] != '';

SELECT 'Map fixed full text bloom filter tokenbf mapKeys';

SELECT 'Equals with existing key';
SELECT * FROM bf_tokenbf_map_values_test WHERE map_fixed['K0'] = 'V0' SETTINGS force_data_skipping_indices='map_fixed_values_tokenbf';
SELECT 'Equals with non existing key';
SELECT * FROM bf_tokenbf_map_values_test WHERE map_fixed['K2'] = 'V2' SETTINGS force_data_skipping_indices='map_fixed_values_tokenbf';
SELECT 'Equals with non existing key and default value';
SELECT * FROM bf_tokenbf_map_values_test WHERE map_fixed['K3'] = '';
SELECT 'Not equals with existing key';
SELECT * FROM bf_tokenbf_map_values_test WHERE map_fixed['K0'] != 'V0' SETTINGS force_data_skipping_indices='map_fixed_values_tokenbf';
SELECT 'Not equals with non existing key';
SELECT * FROM bf_tokenbf_map_values_test WHERE map_fixed['K2'] != 'V2' SETTINGS force_data_skipping_indices='map_fixed_values_tokenbf';
SELECT 'Not equals with non existing key and default value';
SELECT * FROM bf_tokenbf_map_values_test WHERE map_fixed['K3'] != '';

DROP TABLE bf_tokenbf_map_values_test;

CREATE TABLE bf_ngrambf_map_keys_test
(
    row_id UInt32,
    map Map(String, String),
    map_fixed Map(FixedString(2), String),
    INDEX map_keys_ngrambf mapKeys(map) TYPE ngrambf_v1(4,256,2,0) GRANULARITY 1,
    INDEX map_fixed_keys_ngrambf mapKeys(map_fixed) TYPE ngrambf_v1(4,256,2,0) GRANULARITY 1
) Engine=MergeTree() ORDER BY row_id SETTINGS index_granularity = 1;

INSERT INTO bf_ngrambf_map_keys_test VALUES (0, {'K0':'V0'}, {'K0':'V0'}), (1, {'K1':'V1'}, {'K1':'V1'});

SELECT 'Map full text bloom filter ngrambf mapKeys';

SELECT 'Equals with existing key';
SELECT * FROM bf_ngrambf_map_keys_test WHERE map['K0'] = 'V0' SETTINGS force_data_skipping_indices='map_keys_ngrambf';
SELECT 'Equals with non existing key';
SELECT * FROM bf_ngrambf_map_keys_test WHERE map['K2'] = 'V2' SETTINGS force_data_skipping_indices='map_keys_ngrambf';
SELECT 'Equals with non existing key and default value';
SELECT * FROM bf_ngrambf_map_keys_test WHERE map['K3'] = '';
SELECT 'Not equals with existing key';
SELECT * FROM bf_ngrambf_map_keys_test WHERE map['K0'] != 'V0' SETTINGS force_data_skipping_indices='map_keys_ngrambf';
SELECT 'Not equals with non existing key';
SELECT * FROM bf_ngrambf_map_keys_test WHERE map['K2'] != 'V2' SETTINGS force_data_skipping_indices='map_keys_ngrambf';
SELECT 'Not equals with non existing key and default value';
SELECT * FROM bf_ngrambf_map_keys_test WHERE map['K3'] != '';

SELECT 'Map fixed full text bloom filter ngrambf mapKeys';

SELECT 'Equals with existing key';
SELECT * FROM bf_ngrambf_map_keys_test WHERE map_fixed['K0'] = 'V0' SETTINGS force_data_skipping_indices='map_fixed_keys_ngrambf';
SELECT 'Equals with non existing key';
SELECT * FROM bf_ngrambf_map_keys_test WHERE map_fixed['K2'] = 'V2' SETTINGS force_data_skipping_indices='map_fixed_keys_ngrambf';
SELECT 'Equals with non existing key and default value';
SELECT * FROM bf_ngrambf_map_keys_test WHERE map_fixed['K3'] = '';
SELECT 'Not equals with existing key';
SELECT * FROM bf_ngrambf_map_keys_test WHERE map_fixed['K0'] != 'V0' SETTINGS force_data_skipping_indices='map_fixed_keys_ngrambf';
SELECT 'Not equals with non existing key';
SELECT * FROM bf_ngrambf_map_keys_test WHERE map_fixed['K2'] != 'V2' SETTINGS force_data_skipping_indices='map_fixed_keys_ngrambf';
SELECT 'Not equals with non existing key and default value';
SELECT * FROM bf_ngrambf_map_keys_test WHERE map_fixed['K3'] != '';

DROP TABLE bf_ngrambf_map_keys_test;

CREATE TABLE bf_ngrambf_map_values_test
(
    row_id UInt32,
    map Map(String, String),
    map_fixed Map(FixedString(2), String),
    INDEX map_values_ngrambf mapKeys(map) TYPE ngrambf_v1(4,256,2,0) GRANULARITY 1,
    INDEX map_fixed_values_ngrambf mapKeys(map_fixed) TYPE ngrambf_v1(4,256,2,0) GRANULARITY 1
) Engine=MergeTree() ORDER BY row_id SETTINGS index_granularity = 1;

INSERT INTO bf_ngrambf_map_values_test VALUES (0, {'K0':'V0'}, {'K0':'V0'}), (1, {'K1':'V1'}, {'K1':'V1'});

SELECT 'Map full text bloom filter ngrambf mapValues';

SELECT 'Equals with existing key';
SELECT * FROM bf_ngrambf_map_values_test WHERE map['K0'] = 'V0' SETTINGS force_data_skipping_indices='map_values_ngrambf';
SELECT 'Equals with non existing key';
SELECT * FROM bf_ngrambf_map_values_test WHERE map['K2'] = 'V2' SETTINGS force_data_skipping_indices='map_values_ngrambf';
SELECT 'Equals with non existing key and default value';
SELECT * FROM bf_ngrambf_map_values_test WHERE map['K3'] = '';
SELECT 'Not equals with existing key';
SELECT * FROM bf_ngrambf_map_values_test WHERE map['K0'] != 'V0' SETTINGS force_data_skipping_indices='map_values_ngrambf';
SELECT 'Not equals with non existing key';
SELECT * FROM bf_ngrambf_map_values_test WHERE map['K2'] != 'V2' SETTINGS force_data_skipping_indices='map_values_ngrambf';
SELECT 'Not equals with non existing key and default value';
SELECT * FROM bf_ngrambf_map_values_test WHERE map['K3'] != '';

SELECT 'Map fixed full text bloom filter ngrambf mapKeys';

SELECT 'Equals with existing key';
SELECT * FROM bf_ngrambf_map_values_test WHERE map_fixed['K0'] = 'V0' SETTINGS force_data_skipping_indices='map_fixed_values_ngrambf';
SELECT 'Equals with non existing key';
SELECT * FROM bf_ngrambf_map_values_test WHERE map_fixed['K2'] = 'V2' SETTINGS force_data_skipping_indices='map_fixed_values_ngrambf';
SELECT 'Equals with non existing key and default value';
SELECT * FROM bf_ngrambf_map_values_test WHERE map_fixed['K3'] = '';
SELECT 'Not equals with existing key';
SELECT * FROM bf_ngrambf_map_values_test WHERE map_fixed['K0'] != 'V0' SETTINGS force_data_skipping_indices='map_fixed_values_ngrambf';
SELECT 'Not equals with non existing key';
SELECT * FROM bf_ngrambf_map_values_test WHERE map_fixed['K2'] != 'V2' SETTINGS force_data_skipping_indices='map_fixed_values_ngrambf';
SELECT 'Not equals with non existing key and default value';
SELECT * FROM bf_ngrambf_map_values_test WHERE map_fixed['K3'] != '';

DROP TABLE bf_ngrambf_map_values_test;
