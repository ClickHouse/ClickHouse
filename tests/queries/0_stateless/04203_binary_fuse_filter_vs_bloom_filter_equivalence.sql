-- Tags: no-parallel, no-random-settings, no-random-merge-tree-settings
-- Bloom vs binary_fuse skip index: same target FPR, same rows, same PREWHERE semantics.

SET allow_experimental_binary_fuse_filter_index = 1;

DROP TABLE IF EXISTS t_skip_bloom_binary_fuse;
DROP TABLE IF EXISTS t_skip_binary_fuse;

CREATE TABLE t_skip_bloom_binary_fuse
(
    k UInt64,
    v UInt64,
    INDEX idx_v v TYPE bloom_filter(0.01) GRANULARITY 1
)
ENGINE = MergeTree()
ORDER BY k;

CREATE TABLE t_skip_binary_fuse
(
    k UInt64,
    v UInt64,
    INDEX idx_v v TYPE binary_fuse_filter(0.01) GRANULARITY 1
)
ENGINE = MergeTree()
ORDER BY k;

SYSTEM STOP MERGES t_skip_bloom_binary_fuse;
SYSTEM STOP MERGES t_skip_binary_fuse;

INSERT INTO t_skip_bloom_binary_fuse SELECT number, number FROM numbers(300000);
INSERT INTO t_skip_binary_fuse SELECT number, number FROM numbers(300000);

SELECT '-- row exists';
SELECT (SELECT v FROM t_skip_bloom_binary_fuse WHERE k = 1) = (SELECT v FROM t_skip_binary_fuse WHERE k = 1);

SELECT '-- match on existing v (counts equal)';
WITH CAST(42 AS UInt64) AS probe
SELECT (SELECT count() FROM t_skip_bloom_binary_fuse PREWHERE v = probe) = (SELECT count() FROM t_skip_binary_fuse PREWHERE v = probe);

SELECT '-- match on absent v (counts equal)';
SELECT (SELECT count() FROM t_skip_bloom_binary_fuse PREWHERE v = toUInt64(0xDEADBEEFDEADBEEF))
    = (SELECT count() FROM t_skip_binary_fuse PREWHERE v = toUInt64(0xDEADBEEFDEADBEEF));

SYSTEM START MERGES t_skip_bloom_binary_fuse;
SYSTEM START MERGES t_skip_binary_fuse;

DROP TABLE t_skip_bloom_binary_fuse;
DROP TABLE t_skip_binary_fuse;
