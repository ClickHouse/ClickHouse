
DROP TABLE IF EXISTS test_map_contains_values;

CREATE TABLE test_map_contains_values
(
    `ResourceAttributes` Map(LowCardinality(String), String),
    INDEX idx_res_attr_value mapValues(ResourceAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
)
ORDER BY tuple();

INSERT INTO test_map_contains_values VALUES ( { 'rum.sessionId': 'session123' } );

DROP TABLE IF EXISTS test_map_contains_keys;

CREATE TABLE test_map_contains_keys
(
    `ResourceAttributes` Map(LowCardinality(String), String),
    INDEX idx_res_attr_value mapKeys(ResourceAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
)
ORDER BY tuple();

INSERT INTO test_map_contains_keys VALUES ( { 'rum.sessionId': 'session123' } );


SELECT * FROM test_map_contains_values WHERE mapContains(ResourceAttributes, 'rum.sessionId');
SELECT * FROM test_map_contains_values WHERE mapContainsKey(ResourceAttributes, 'rum.sessionId');
SELECT * FROM test_map_contains_values WHERE mapContainsValue(ResourceAttributes, 'session123');
SELECT * FROM test_map_contains_keys WHERE mapContains(ResourceAttributes, 'rum.sessionId');
SELECT * FROM test_map_contains_keys WHERE mapContainsKey(ResourceAttributes, 'rum.sessionId');
SELECT * FROM test_map_contains_keys WHERE mapContainsValue(ResourceAttributes, 'session123');
