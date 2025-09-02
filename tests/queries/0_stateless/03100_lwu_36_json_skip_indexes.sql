DROP TABLE IF EXISTS test;

CREATE TABLE test (
    id UInt64,
    document JSON(name String, age UInt16),
    INDEX ix_name document.name TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX ix_country document.country::String TYPE bloom_filter(0.01) GRANULARITY 1
)
ENGINE = MergeTree()
ORDER BY (id)
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, index_granularity = 1;

INSERT INTO test VALUES (1, '{"name":"foo", "age":15}');
INSERT INTO test VALUES (2, '{"name":"boo", "age":15}');
INSERT INTO test VALUES (3, '{"name":"bar", "age":15}');

SET allow_experimental_lightweight_update = 1;

UPDATE test SET document = '{"name":"aaa", "age":15, "country": "USA"}' WHERE id = 1;

SELECT * FROM test
WHERE document.name = 'aaa' OR document.name = 'boo'
ORDER BY id
SETTINGS apply_patch_parts = 1;

SELECT * FROM test
WHERE document.name = 'aaa' OR document.name = 'boo'
ORDER BY id
SETTINGS apply_patch_parts = 1, force_data_skipping_indices = 'ix_name'; -- { serverError INDEX_NOT_USED }

SELECT * FROM test
WHERE document.name = 'aaa' OR document.name = 'boo'
ORDER BY id
SETTINGS apply_patch_parts = 0, force_data_skipping_indices = 'ix_name';

SELECT count()FROM test
WHERE document.country::String = 'USA'
SETTINGS apply_patch_parts = 1;

SELECT count() FROM test
WHERE document.country::String = 'USA'
SETTINGS apply_patch_parts = 1, force_data_skipping_indices = 'ix_country'; -- { serverError INDEX_NOT_USED }

SELECT count() FROM test
WHERE document.country::String = 'USA'
SETTINGS apply_patch_parts = 0, force_data_skipping_indices = 'ix_country';

DROP TABLE IF EXISTS test;
