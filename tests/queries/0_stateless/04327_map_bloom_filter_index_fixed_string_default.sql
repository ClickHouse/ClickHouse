DROP TABLE IF EXISTS t_map_bf_fixed_string;

CREATE TABLE t_map_bf_fixed_string
(
    row_id UInt32,
    map Map(String, String),
    INDEX idx mapKeys(map) TYPE bloom_filter GRANULARITY 1
)
ENGINE = MergeTree ORDER BY row_id SETTINGS index_granularity = 1;

INSERT INTO t_map_bf_fixed_string VALUES (0, {'K0':'V0'}), (1, {'K1':'V1'});

SELECT 'Absent key compared with FixedString default (matches String default at runtime)';
SELECT count() FROM t_map_bf_fixed_string WHERE map[''] = toFixedString('', 3);
SELECT count() FROM t_map_bf_fixed_string WHERE map[''] = toFixedString('', 3) SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_map_bf_fixed_string WHERE map[''] = '';

SELECT 'Present key with FixedString constant still uses the index';
SELECT count() FROM t_map_bf_fixed_string WHERE map['K0'] = toFixedString('V0', 2) SETTINGS force_data_skipping_indices = 'idx';

SELECT 'Absent key with non-default FixedString constant is still pruned';
SELECT count() FROM t_map_bf_fixed_string WHERE map['K2'] = toFixedString('V2', 2) SETTINGS force_data_skipping_indices = 'idx';

DROP TABLE t_map_bf_fixed_string;

DROP TABLE IF EXISTS t_map_bf_int_default;

CREATE TABLE t_map_bf_int_default
(
    row_id UInt32,
    map Map(String, UInt64),
    INDEX idx mapKeys(map) TYPE bloom_filter GRANULARITY 1
)
ENGINE = MergeTree ORDER BY row_id SETTINGS index_granularity = 1;

INSERT INTO t_map_bf_int_default VALUES (0, {'K0':10}), (1, {'K1':20});

SELECT 'Absent key compared with integer default 0 via different integer type';
SELECT count() FROM t_map_bf_int_default WHERE map['K2'] = toInt8(0);
SELECT count() FROM t_map_bf_int_default WHERE map['K2'] = toInt8(0) SETTINGS use_skip_indexes = 0;

SELECT 'Absent key compared with non-default integer is still pruned';
SELECT count() FROM t_map_bf_int_default WHERE map['K2'] = toInt8(5) SETTINGS force_data_skipping_indices = 'idx';

DROP TABLE t_map_bf_int_default;
