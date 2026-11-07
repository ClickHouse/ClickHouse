-- Tags: no-random-settings, no-random-merge-tree-settings
-- Bloom vs cuckoo skip index: same target FPR (0.01), same rows — PREWHERE counts must match.
-- This file checks semantic equivalence between index types, not relative performance or size.

SET allow_experimental_cuckoo_filter_index = 1;

DROP TABLE IF EXISTS t_skip_bloom;
DROP TABLE IF EXISTS t_skip_cuckoo;

CREATE TABLE t_skip_bloom
(
    k UInt64,
    v UInt64,
    INDEX idx_v v TYPE bloom_filter(0.01) GRANULARITY 1
)
ENGINE = MergeTree()
ORDER BY k;

CREATE TABLE t_skip_cuckoo
(
    k UInt64,
    v UInt64,
    INDEX idx_v v TYPE cuckoo_filter(0.01) GRANULARITY 1
)
ENGINE = MergeTree()
ORDER BY k;

SYSTEM STOP MERGES t_skip_bloom;
SYSTEM STOP MERGES t_skip_cuckoo;

-- v = k: unique per row, no hash collisions (unlike cityHash64).
INSERT INTO t_skip_bloom SELECT number, number FROM numbers(300000);
INSERT INTO t_skip_cuckoo SELECT number, number FROM numbers(300000);

SELECT '-- row exists';
SELECT (SELECT v FROM t_skip_bloom WHERE k = 1) = (SELECT v FROM t_skip_cuckoo WHERE k = 1);

SELECT '-- match on existing v (counts equal, no fixed count in reference)';
WITH CAST(42 AS UInt64) AS probe
SELECT (SELECT count() FROM t_skip_bloom PREWHERE v = probe) = (SELECT count() FROM t_skip_cuckoo PREWHERE v = probe);

SELECT '-- match on absent v (counts equal)';
SELECT (SELECT count() FROM t_skip_bloom PREWHERE v = toUInt64(0xDEADBEEFDEADBEEF))
    = (SELECT count() FROM t_skip_cuckoo PREWHERE v = toUInt64(0xDEADBEEFDEADBEEF));

SYSTEM START MERGES t_skip_bloom;
SYSTEM START MERGES t_skip_cuckoo;

DROP TABLE t_skip_bloom;
DROP TABLE t_skip_cuckoo;
