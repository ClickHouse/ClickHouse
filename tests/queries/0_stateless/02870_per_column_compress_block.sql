-- Tags: no-random-merge-tree-settings

CREATE TABLE t
(
    `id` UInt64 CODEC(ZSTD(1)),
    `long_string` String CODEC(ZSTD(9, 24)) SETTINGS (min_compress_block_size = 163840, max_compress_block_size = 163840),
    `v1` String CODEC(ZSTD(1)),
    `v2` UInt64 CODEC(ZSTD(1)),
    `v3` Float32 CODEC(ZSTD(1)),
    `v4` Float64 CODEC(ZSTD(1))
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t/2870', 'r1')
ORDER BY id
SETTINGS min_bytes_for_wide_part = 1;

SHOW CREATE t;

INSERT INTO TABLE t SELECT number, randomPrintableASCII(1000), randomPrintableASCII(10), rand(number), rand(number+1), rand(number+2) FROM numbers(1000);

SELECT count() FROM t;

ALTER TABLE t MODIFY COLUMN long_string MODIFY SETTING min_compress_block_size = 8192;

SHOW CREATE t;

ALTER TABLE t MODIFY COLUMN long_string RESET SETTING min_compress_block_size;

SHOW CREATE t;

ALTER TABLE t MODIFY COLUMN long_string REMOVE SETTINGS;

SHOW CREATE t;

ALTER TABLE t MODIFY COLUMN long_string String CODEC(ZSTD(9, 24)) SETTINGS (min_compress_block_size = 163840, max_compress_block_size = 163840);

SHOW CREATE t;

DROP TABLE t;

SET allow_experimental_object_type = 1;

CREATE TABLE t2
(
    `id` UInt64 CODEC(ZSTD(1)),
    `tup` Tuple(UInt64, UInt64) CODEC(ZSTD(1)) SETTINGS (min_compress_block_size = 81920, max_compress_block_size = 163840),
    `json` JSON CODEC(ZSTD(9, 24)) SETTINGS (min_compress_block_size = 81920, max_compress_block_size = 163840),
)
ENGINE = MergeTree
ORDER BY id
SETTINGS min_bytes_for_wide_part = 1;

INSERT INTO TABLE t2 SELECT number, tuple(number, number), concat('{"key": ', toString(number), ' ,"value": ', toString(rand(number+1)), '}') FROM numbers(1000);
SELECT tup, json.key AS key FROM t2 ORDER BY key LIMIT 10;

DROP TABLE t2;

-- Non-supported column setting
CREATE TABLE t3
(
    `id` UInt64 CODEC(ZSTD(1)),
    `long_string` String CODEC(ZSTD(1)) SETTINGS (min_block_size = 81920, max_compress_block_size = 163840),
)
ENGINE = MergeTree
ORDER BY id
SETTINGS min_bytes_for_wide_part = 1; -- {serverError 115}

-- Invalid setting values
CREATE TABLE t4
(
    `id` UInt64 CODEC(ZSTD(1)),
    `long_string` String CODEC(ZSTD(1)) SETTINGS (min_compress_block_size = 81920, max_compress_block_size = 163840),
)
ENGINE = TinyLog
ORDER BY id; -- {serverError 44}

