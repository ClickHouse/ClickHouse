-- Tags: no-random-merge-tree-settings
-- ^ test asserts exact part levels, which randomized merge tree settings can perturb

-- Adopting a part from a plain MergeTree into a collapsing engine (Replacing/Summing/
-- Aggregating) used to keep the source part's merge level. A lone level>0 part is treated
-- as fully merged and skipped by FINAL/OPTIMIZE, so duplicate ORDER BY keys survived
-- (issue #106798). The adopted part's level is now reset to 0 unless this table would merge
-- the part with identical semantics, so FINAL/OPTIMIZE deduplicate it. The level is only
-- preserved when the full merge semantics match (mode and every MergingParams field), not
-- just the mode: same-mode tables can differ in columns_to_sum / is_deleted_column / etc.,
-- and the destination would otherwise treat a part merged under different semantics as
-- already-merged.

DROP TABLE IF EXISTS src_mt;
DROP TABLE IF EXISTS dst_rmt_clone;
DROP TABLE IF EXISTS dst_rmt_attach;
DROP TABLE IF EXISTS dst_smt;
DROP TABLE IF EXISTS src_mt_agg;
DROP TABLE IF EXISTS dst_amt;
DROP TABLE IF EXISTS src_mt_move;
DROP TABLE IF EXISTS dst_rmt_move;
DROP TABLE IF EXISTS src_rmt_same;
DROP TABLE IF EXISTS dst_rmt_same;
DROP TABLE IF EXISTS src_smt_b;
DROP TABLE IF EXISTS dst_smt_c;
DROP TABLE IF EXISTS src_rmt_ver;
DROP TABLE IF EXISTS dst_rmt_ver_del;

-- A source MergeTree part at level 1 (merged, but no dedup under MergeTree semantics).
CREATE TABLE src_mt (a UInt32, b UInt32) ENGINE = MergeTree ORDER BY a;
INSERT INTO src_mt VALUES (1, 10);
INSERT INTO src_mt VALUES (1, 20);
OPTIMIZE TABLE src_mt FINAL;
SELECT 'src level', max(level) FROM system.parts WHERE database = currentDatabase() AND table = 'src_mt' AND active;

-- CLONE AS into ReplacingMergeTree: adopted part must be reset to level 0 and dedup.
CREATE TABLE dst_rmt_clone CLONE AS src_mt ENGINE = ReplacingMergeTree;
SELECT 'clone level', max(level) FROM system.parts WHERE database = currentDatabase() AND table = 'dst_rmt_clone' AND active;
SELECT 'clone final', count() FROM dst_rmt_clone FINAL;
OPTIMIZE TABLE dst_rmt_clone FINAL;
SELECT 'clone after optimize', count() FROM dst_rmt_clone FINAL;

-- ATTACH PARTITION FROM into ReplacingMergeTree.
CREATE TABLE dst_rmt_attach (a UInt32, b UInt32) ENGINE = ReplacingMergeTree ORDER BY a;
ALTER TABLE dst_rmt_attach ATTACH PARTITION tuple() FROM src_mt;
SELECT 'attach level', max(level) FROM system.parts WHERE database = currentDatabase() AND table = 'dst_rmt_attach' AND active;
SELECT 'attach final', count() FROM dst_rmt_attach FINAL;

-- CLONE AS into SummingMergeTree: rows with the same key must sum.
CREATE TABLE dst_smt CLONE AS src_mt ENGINE = SummingMergeTree;
SELECT 'summing final', a, b FROM dst_smt FINAL ORDER BY a;

-- ATTACH PARTITION FROM into AggregatingMergeTree (source column type matches the target's
-- SimpleAggregateFunction state, as ATTACH PARTITION FROM requires identical structure).
CREATE TABLE src_mt_agg (a UInt32, b SimpleAggregateFunction(sum, UInt64)) ENGINE = MergeTree ORDER BY a;
INSERT INTO src_mt_agg VALUES (1, 10);
INSERT INTO src_mt_agg VALUES (1, 20);
OPTIMIZE TABLE src_mt_agg FINAL;
CREATE TABLE dst_amt (a UInt32, b SimpleAggregateFunction(sum, UInt64)) ENGINE = AggregatingMergeTree ORDER BY a;
ALTER TABLE dst_amt ATTACH PARTITION tuple() FROM src_mt_agg;
SELECT 'aggregating level', max(level) FROM system.parts WHERE database = currentDatabase() AND table = 'dst_amt' AND active;
SELECT 'aggregating final', a, b FROM dst_amt FINAL ORDER BY a;

-- MOVE PARTITION TO TABLE (MergeTree -> ReplacingMergeTree).
CREATE TABLE src_mt_move (a UInt32, b UInt32) ENGINE = MergeTree ORDER BY a;
INSERT INTO src_mt_move VALUES (1, 10);
INSERT INTO src_mt_move VALUES (1, 20);
OPTIMIZE TABLE src_mt_move FINAL;
CREATE TABLE dst_rmt_move (a UInt32, b UInt32) ENGINE = ReplacingMergeTree ORDER BY a;
ALTER TABLE src_mt_move MOVE PARTITION tuple() TO TABLE dst_rmt_move;
SELECT 'move level', max(level) FROM system.parts WHERE database = currentDatabase() AND table = 'dst_rmt_move' AND active;
SELECT 'move final', count() FROM dst_rmt_move FINAL;

-- Same engine on both sides: the part keeps its level (nothing to re-merge, and resetting
-- would lose the optimize_on_insert signal). Verify the level is preserved, not reset.
CREATE TABLE src_rmt_same (a UInt32, b UInt32) ENGINE = ReplacingMergeTree ORDER BY a;
INSERT INTO src_rmt_same VALUES (1, 10);
INSERT INTO src_rmt_same VALUES (1, 20);
OPTIMIZE TABLE src_rmt_same FINAL;
CREATE TABLE dst_rmt_same (a UInt32, b UInt32) ENGINE = ReplacingMergeTree ORDER BY a;
ALTER TABLE dst_rmt_same ATTACH PARTITION tuple() FROM src_rmt_same;
SELECT 'same engine level preserved', max(level) > 0 FROM system.parts WHERE database = currentDatabase() AND table = 'dst_rmt_same' AND active;
SELECT 'same engine final', count() FROM dst_rmt_same FINAL;

-- Same mode (Summing), DIFFERENT columns_to_sum. checkStructureAndGetMergeTreeData does not
-- compare MergingParams, so this ATTACH is allowed. The source row (2, 9, 0) has its
-- destination-summed column c = 0; under the destination's columns_to_sum = c it is an
-- all-zero row that must be dropped on merge. If the level>0 part were preserved, OPTIMIZE
-- with optimize_skip_merged_partitions would skip the lone part and the row would survive.
CREATE TABLE src_smt_b (k UInt32, b Int64, c Int64) ENGINE = SummingMergeTree(b) ORDER BY k;
INSERT INTO src_smt_b VALUES (1, 5, 7);
INSERT INTO src_smt_b VALUES (2, 9, 0);
OPTIMIZE TABLE src_smt_b FINAL;
CREATE TABLE dst_smt_c (k UInt32, b Int64, c Int64) ENGINE = SummingMergeTree(c) ORDER BY k;
ALTER TABLE dst_smt_c ATTACH PARTITION tuple() FROM src_smt_b;
SELECT 'summing diff columns_to_sum level', max(level) FROM system.parts WHERE database = currentDatabase() AND table = 'dst_smt_c' AND active;
OPTIMIZE TABLE dst_smt_c FINAL SETTINGS optimize_skip_merged_partitions = 1;
SELECT 'summing diff columns_to_sum rows', k, b, c FROM dst_smt_c ORDER BY k;

-- Same mode (Replacing), DIFFERENT is_deleted_column. The source has no is_deleted handling;
-- the destination uses del as is_deleted, so a level>0 part merged under the source's
-- semantics must not be trusted as already-merged. Reset to 0 lets FINAL apply the
-- destination's is_deleted cleanup.
CREATE TABLE src_rmt_ver (k UInt32, ver UInt64, del UInt8) ENGINE = ReplacingMergeTree(ver) ORDER BY k;
INSERT INTO src_rmt_ver VALUES (1, 1, 0);
INSERT INTO src_rmt_ver VALUES (1, 2, 1);
OPTIMIZE TABLE src_rmt_ver FINAL;
CREATE TABLE dst_rmt_ver_del (k UInt32, ver UInt64, del UInt8) ENGINE = ReplacingMergeTree(ver, del) ORDER BY k;
ALTER TABLE dst_rmt_ver_del ATTACH PARTITION tuple() FROM src_rmt_ver;
SELECT 'replacing diff is_deleted level', max(level) FROM system.parts WHERE database = currentDatabase() AND table = 'dst_rmt_ver_del' AND active;
SELECT 'replacing diff is_deleted final', count() FROM dst_rmt_ver_del FINAL;

DROP TABLE src_mt;
DROP TABLE dst_rmt_clone;
DROP TABLE dst_rmt_attach;
DROP TABLE dst_smt;
DROP TABLE src_mt_agg;
DROP TABLE dst_amt;
DROP TABLE src_mt_move;
DROP TABLE dst_rmt_move;
DROP TABLE src_rmt_same;
DROP TABLE dst_rmt_same;
DROP TABLE src_smt_b;
DROP TABLE dst_smt_c;
DROP TABLE src_rmt_ver;
DROP TABLE dst_rmt_ver_del;
