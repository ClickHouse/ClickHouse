-- Tags: no-random-merge-tree-settings
CREATE TABLE t
(
    `id` UInt64 CODEC(ZSTD(1)),
    `long_string` String CODEC(ZSTD(9, 24)) COMPRESS BLOCK (67108864, 67108864),
    `v1` String CODEC(ZSTD(1)),
    `v2` UInt64 CODEC(ZSTD(1)),
    `v3` Float32 CODEC(ZSTD(1)),
    `v4` Float64 CODEC(ZSTD(1))
)
ENGINE = MergeTree
ORDER BY id
SETTINGS min_bytes_for_wide_part = 1;

INSERT INTO TABLE t SELECT number, randomPrintableASCII(1000), randomPrintableASCII(10), rand(number), rand(number+1), rand(number+2) FROM numbers(1000);

SELECT count() FROM t;

SET allow_experimental_object_type = 1;

CREATE TABLE t2
(
    `id` UInt64 CODEC(ZSTD(1)),
    `tup` Tuple(UInt64, UInt64) CODEC(ZSTD(1)) COMPRESS BLOCK (1024, 8192),
    `json` JSON CODEC(ZSTD(9, 24)) COMPRESS BLOCK (671088, 671088),
)
ENGINE = MergeTree
ORDER BY id
SETTINGS min_bytes_for_wide_part = 1;

INSERT INTO TABLE t2 SELECT number, tuple(number, number), concat('{"key": ', toString(number), ' ,"value": ', toString(rand(number+1)), '}') FROM numbers(1000);
SELECT tup, json.key AS key FROM t2 ORDER BY key LIMIT 10;
