-- Tags: no-random-merge-tree-settings, no-replicated-database
-- ^ test asserts exact part levels, which randomized merge tree settings can perturb
-- Tag no-replicated-database: `CREATE CLONE AS is not supported with Replicated databases`

-- A part adopted into a collapsing engine counts as already merged while its level stays >0, so
-- `FINAL`/`OPTIMIZE` skip it and duplicate keys survive. The level has to be reset to 0.
-- The `ALTER ... ATTACH/MOVE PARTITION` forms are in 04001_attach_clone_part_level_reset.sql.

DROP TABLE IF EXISTS src_mt;
DROP TABLE IF EXISTS dst_rmt_clone;
DROP TABLE IF EXISTS dst_smt;

-- A source `MergeTree` part at level 1 (merged, but no dedup under `MergeTree` semantics).
CREATE TABLE src_mt (a UInt32, b UInt32) ENGINE = MergeTree ORDER BY a;
INSERT INTO src_mt VALUES (1, 10);
INSERT INTO src_mt VALUES (1, 20);
OPTIMIZE TABLE src_mt FINAL;
SELECT 'src level', max(level) FROM system.parts WHERE database = currentDatabase() AND table = 'src_mt' AND active;

-- `CLONE AS` into `ReplacingMergeTree`: adopted part must be reset to level 0 and dedup.
CREATE TABLE dst_rmt_clone CLONE AS src_mt ENGINE = ReplacingMergeTree;
SELECT 'clone level', max(level) FROM system.parts WHERE database = currentDatabase() AND table = 'dst_rmt_clone' AND active;
SELECT 'clone final', count() FROM dst_rmt_clone FINAL;
OPTIMIZE TABLE dst_rmt_clone FINAL;
SELECT 'clone after optimize', count() FROM dst_rmt_clone FINAL;

-- `CLONE AS` into `SummingMergeTree`: rows with the same key must sum.
CREATE TABLE dst_smt CLONE AS src_mt ENGINE = SummingMergeTree;
SELECT 'summing final', a, b FROM dst_smt FINAL ORDER BY a;

DROP TABLE src_mt;
DROP TABLE dst_rmt_clone;
DROP TABLE dst_smt;
