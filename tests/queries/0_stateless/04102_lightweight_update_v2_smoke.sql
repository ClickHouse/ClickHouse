-- v2 lightweight-update smoke test. Covers:
--  1. Basic UPDATE with enable_v2_lightweight_update_patches on, simple single-column sort key.
--  2. Composite sort key.
--  3. Expression sort key (v2 persists physical source columns and replays the expression at
--     apply time — same pattern FINAL uses, no fallback needed).
--  4. Update that crosses a merge boundary (the case that previously forced Join mode).

DROP TABLE IF EXISTS t_v2_simple;
DROP TABLE IF EXISTS t_v2_composite;
DROP TABLE IF EXISTS t_v2_expr;
DROP TABLE IF EXISTS t_v2_cross_merge;

-- (1) Simple sort key -------------------------------------------------------

CREATE TABLE t_v2_simple (id UInt64, payload UInt64)
ENGINE = MergeTree
ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, enable_v2_lightweight_update_patches = 1;

INSERT INTO t_v2_simple SELECT number, number FROM numbers(1000);
UPDATE t_v2_simple SET payload = 42 WHERE id % 100 = 0;

SELECT 'v2-simple', count() FROM t_v2_simple WHERE payload = 42;
SELECT 'v2-simple-check', count() FROM t_v2_simple WHERE payload = id AND id % 100 != 0;

-- (2) Composite sort key ---------------------------------------------------

CREATE TABLE t_v2_composite (a UInt64, b UInt64, payload String)
ENGINE = MergeTree
ORDER BY (a, b)
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, enable_v2_lightweight_update_patches = 1;

INSERT INTO t_v2_composite SELECT number % 10, number, 'initial' FROM numbers(500);
UPDATE t_v2_composite SET payload = 'patched' WHERE a = 3;

SELECT 'v2-composite', count() FROM t_v2_composite WHERE payload = 'patched';
SELECT 'v2-composite-check', count() FROM t_v2_composite WHERE payload = 'initial' AND a != 3;

-- (3) Expression sort key — v2 stores physical source column (`id`) and recomputes
--     `intHash32(id)` via the KeyDescription's ExpressionActions at patch-apply time.

CREATE TABLE t_v2_expr (id UInt64, payload UInt64)
ENGINE = MergeTree
ORDER BY (intHash32(id))
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, enable_v2_lightweight_update_patches = 1;

INSERT INTO t_v2_expr SELECT number, number FROM numbers(200);
UPDATE t_v2_expr SET payload = 7 WHERE id < 20;

SELECT 'v2-expr-fallback', count() FROM t_v2_expr WHERE payload = 7;
SELECT 'v2-expr-fallback-check', count() FROM t_v2_expr WHERE payload = id AND id >= 20;

-- (4) Cross-merge-boundary scenario --------------------------------------

CREATE TABLE t_v2_cross_merge (id UInt64, payload UInt64)
ENGINE = MergeTree
ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, enable_v2_lightweight_update_patches = 1;

SYSTEM STOP MERGES t_v2_cross_merge;
INSERT INTO t_v2_cross_merge SELECT number, number FROM numbers(0, 500);
INSERT INTO t_v2_cross_merge SELECT number, number FROM numbers(500, 500);

-- Update straddles both insert blocks.
UPDATE t_v2_cross_merge SET payload = 99 WHERE id BETWEEN 400 AND 600;

SYSTEM START MERGES t_v2_cross_merge;
OPTIMIZE TABLE t_v2_cross_merge FINAL;

SELECT 'v2-cross-merge', count() FROM t_v2_cross_merge WHERE payload = 99;
SELECT 'v2-cross-merge-low', count() FROM t_v2_cross_merge WHERE payload = id AND id < 400;
SELECT 'v2-cross-merge-high', count() FROM t_v2_cross_merge WHERE payload = id AND id > 600;

DROP TABLE t_v2_simple;
DROP TABLE t_v2_composite;
DROP TABLE t_v2_expr;
DROP TABLE t_v2_cross_merge;
