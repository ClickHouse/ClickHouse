CREATE TABLE dict_nested_map_test_table
(
	test_id UInt32,
	type String,
	test_config Array(Map(String, Decimal(28,12))),
	ncp UInt8
)
ENGINE=MergeTree()
ORDER BY test_id;

INSERT INTO dict_nested_map_test_table VALUES (3, 't', [{'l': 0.0, 'h': 10000.0, 't': 0.1}, {'l': 10001.0, 'h': 100000000000000.0, 't': 0.2}], 0);

CREATE DICTIONARY dict_nested_map_dictionary
(
	test_id UInt32,
	type String,
	test_config Array(Map(String, Decimal(28,12))),
	ncp UInt8
)
PRIMARY KEY test_id
SOURCE(CLICKHOUSE(TABLE 'dict_nested_map_test_table'))
LAYOUT(HASHED())
LIFETIME(MIN 1 MAX 1000000);

SELECT dictGet('dict_nested_map_dictionary', 'test_config', toUInt64(3));
