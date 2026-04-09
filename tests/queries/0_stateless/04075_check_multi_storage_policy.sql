
DROP TABLE IF EXISTS test_policy;
CREATE TABLE test_policy (id UInt64)
    ENGINE = MergeTree() ORDER BY id
SETTINGS storage_policy = 'nonexistent_policy', storage_policy = 'default';-- { serverError UNKNOWN_POLICY }

DROP TABLE IF EXISTS test_policy;
CREATE TABLE test_policy (id UInt64)
    ENGINE = MergeTree() ORDER BY id
SETTINGS storage_policy = 'nonexistent_policy';-- { serverError UNKNOWN_POLICY }

DROP TABLE IF EXISTS test_policy;
CREATE TABLE test_policy (id UInt64)
    ENGINE = MergeTree() ORDER BY id
SETTINGS  storage_policy = 'default';
