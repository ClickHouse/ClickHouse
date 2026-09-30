SET enable_lightweight_update = 1;
SET use_query_condition_cache = 0;

-- APPLY PATCHES must rebuild skip indices and projections that depend on a
-- patch-updated column, otherwise a query using the stale index/projection
-- returns wrong results.

-- 1. text index
DROP TABLE IF EXISTS t_lwu_53_text;
CREATE TABLE t_lwu_53_text
(
    id UInt64,
    s String,
    INDEX idx_s s TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 8, min_bytes_for_wide_part = 0, enable_block_number_column = 1, enable_block_offset_column = 1;

INSERT INTO t_lwu_53_text SELECT number, 'nothing here' FROM numbers(100);
UPDATE t_lwu_53_text SET s = 'magic token' WHERE id = 7;
ALTER TABLE t_lwu_53_text APPLY PATCHES SETTINGS mutations_sync = 2;

SELECT 'text skip on ', count() FROM t_lwu_53_text WHERE hasToken(s, 'magic') SETTINGS use_skip_indexes = 1;
SELECT 'text skip off', count() FROM t_lwu_53_text WHERE hasToken(s, 'magic') SETTINGS use_skip_indexes = 0;

DROP TABLE t_lwu_53_text;

-- 2. bloom_filter index
DROP TABLE IF EXISTS t_lwu_53_bf;
CREATE TABLE t_lwu_53_bf
(
    id UInt64,
    s String,
    INDEX idx_s s TYPE bloom_filter(0.001) GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 8, min_bytes_for_wide_part = 0, enable_block_number_column = 1, enable_block_offset_column = 1;

INSERT INTO t_lwu_53_bf SELECT number, 'nothing' FROM numbers(100);
UPDATE t_lwu_53_bf SET s = 'magictoken' WHERE id = 7;
ALTER TABLE t_lwu_53_bf APPLY PATCHES SETTINGS mutations_sync = 2;

SELECT 'bf skip on ', count() FROM t_lwu_53_bf WHERE s = 'magictoken' SETTINGS use_skip_indexes = 1;
SELECT 'bf skip off', count() FROM t_lwu_53_bf WHERE s = 'magictoken' SETTINGS use_skip_indexes = 0;

DROP TABLE t_lwu_53_bf;

-- 3. projection over a patch-updated column
DROP TABLE IF EXISTS t_lwu_53_proj;
CREATE TABLE t_lwu_53_proj
(
    id UInt64,
    cat String,
    val UInt64,
    PROJECTION p (SELECT cat, sum(val) GROUP BY cat)
)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, enable_block_number_column = 1, enable_block_offset_column = 1;

INSERT INTO t_lwu_53_proj SELECT number, 'a', 1 FROM numbers(100);
UPDATE t_lwu_53_proj SET cat = 'b' WHERE id < 10;
ALTER TABLE t_lwu_53_proj APPLY PATCHES SETTINGS mutations_sync = 2;

-- The projection part must be rebuilt so it holds 2 groups ('a', 'b'),
-- otherwise a projection-answered query returns stale results once the
-- spent patch part is dropped.
SELECT 'proj rows', sum(rows) FROM system.projection_parts
WHERE table = 't_lwu_53_proj' AND active AND database = currentDatabase();

SELECT 'proj use ', cat, sum(val) FROM t_lwu_53_proj GROUP BY cat ORDER BY cat SETTINGS optimize_use_projections = 1;
SELECT 'proj skip', cat, sum(val) FROM t_lwu_53_proj GROUP BY cat ORDER BY cat SETTINGS optimize_use_projections = 0;

DROP TABLE t_lwu_53_proj;
