-- 1. Create a table with a nonexistent policy
DROP TABLE IF EXISTS test_bad_policy;
CREATE TABLE test_bad_policy (id UInt64)
    ENGINE = MergeTree() ORDER BY id
SETTINGS storage_policy = 'nonexistent_policy';-- { serverError UNKNOWN_POLICY }

-- 2. Create a table with a existent policy
DROP TABLE IF EXISTS test_good_policy;
CREATE TABLE test_good_policy (id UInt64)
    ENGINE = MergeTree() ORDER BY id
SETTINGS storage_policy = 'default';

-- 3. Modify a nonexistent policy
ALTER TABLE test_good_policy MODIFY SETTING storage_policy = 'nonexistent_policy';-- { serverError UNKNOWN_POLICY }



