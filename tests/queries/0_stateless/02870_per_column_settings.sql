-- Tags: no-random-merge-tree-settings, no-replicated-database
-- Tag no-replicated-database: Old syntax is not allowed
-- The test use replicated table to test serialize and deserialize column with settings declaration on zookeeper
-- Tests column-level settings for MergeTree* tables

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    id UInt64,
    long_string String SETTINGS (min_compress_block_size = 163840, max_compress_block_size = 163840),
    v1 String,
    v2 UInt64,
    v3 Float32,
    v4 Float64
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/tab/2870', 'r1')
ORDER BY id
SETTINGS min_bytes_for_wide_part = 1;

SHOW CREATE tab;

INSERT INTO TABLE tab SELECT number, randomPrintableASCII(1000), randomPrintableASCII(10), rand(number), rand(number+1), rand(number+2) FROM numbers(1000);
SELECT count() FROM tab;

SELECT formatQuery('ALTER TABLE tab MODIFY COLUMN long_string MODIFY SETTING min_compress_block_size = 8192;');
ALTER TABLE tab MODIFY COLUMN long_string MODIFY SETTING min_compress_block_size = 8192;
SHOW CREATE tab;

SELECT formatQuery('ALTER TABLE tab MODIFY COLUMN long_string RESET SETTING min_compress_block_size;');
ALTER TABLE tab MODIFY COLUMN long_string RESET SETTING min_compress_block_size;
SHOW CREATE tab;

SELECT formatQuery('ALTER TABLE tab MODIFY COLUMN long_string REMOVE SETTINGS;');
ALTER TABLE tab MODIFY COLUMN long_string REMOVE SETTINGS;
SHOW CREATE tab;

SELECT formatQuery('ALTER TABLE tab MODIFY COLUMN long_string String SETTINGS (min_compress_block_size = 163840, max_compress_block_size = 163840);');
ALTER TABLE tab MODIFY COLUMN long_string String SETTINGS (min_compress_block_size = 163840, max_compress_block_size = 163840);
SHOW CREATE tab;

DROP TABLE tab;

SELECT '---';

SET allow_experimental_object_type = 1;

CREATE TABLE tab
(
    id UInt64,
    tup Tuple(UInt64, UInt64) SETTINGS (min_compress_block_size = 81920, max_compress_block_size = 163840),
    json Object('json') SETTINGS (min_compress_block_size = 81920, max_compress_block_size = 163840),
)
ENGINE = MergeTree
ORDER BY id
SETTINGS min_bytes_for_wide_part = 1;

INSERT INTO TABLE tab SELECT number, tuple(number, number), concat('{"key": ', toString(number), ' ,"value": ', toString(rand(number+1)), '}') FROM numbers(1000);
SELECT tup, json.key AS key FROM tab ORDER BY key LIMIT 10;

DROP TABLE tab;

SELECT '---';

-- Unsupported column-level settings are rejected
CREATE TABLE tab
(
    id UInt64,
    long_string String SETTINGS (min_block_size = 81920, max_compress_block_size = 163840),
)
ENGINE = MergeTree
ORDER BY id
SETTINGS min_bytes_for_wide_part = 1; -- {serverError UNKNOWN_SETTING}
