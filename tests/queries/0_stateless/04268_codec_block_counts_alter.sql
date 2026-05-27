-- Tags: no-random-merge-tree-settings
-- no-random-merge-tree-settings: random settings could flip the parts to Compact, where codec_block_counts is empty.

DROP TABLE IF EXISTS t_alter;

CREATE TABLE t_alter (a UInt32 CODEC(LZ4))
ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0;

SYSTEM STOP MERGES t_alter;

INSERT INTO t_alter SELECT number FROM numbers(100000);

ALTER TABLE t_alter MODIFY COLUMN a CODEC(T64);

INSERT INTO t_alter SELECT number + 100000 FROM numbers(100000);

-- Before merge: distinct codecs per part.
SELECT
    name,
    mapKeys(codec_block_counts) AS codecs
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_alter' AND active AND column = 'a'
ORDER BY name;

SYSTEM START MERGES t_alter;
OPTIMIZE TABLE t_alter FINAL;

-- After merge: the current codec wins.
SELECT
    mapKeys(codec_block_counts) AS codecs
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_alter' AND active AND column = 'a';

DROP TABLE t_alter;
