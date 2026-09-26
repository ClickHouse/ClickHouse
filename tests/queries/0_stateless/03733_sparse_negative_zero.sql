-- Tags: no-random-merge-tree-settings

-- Verify that sparse serialization preserves -0.0 (negative zero)
-- GitHub issue: https://github.com/ClickHouse/ClickHouse/issues/98637

-- BFloat16
DROP TABLE IF EXISTS t_sparse_neg_zero_bf16;
CREATE TABLE t_sparse_neg_zero_bf16 (f BFloat16) Engine = MergeTree ORDER BY tuple() SETTINGS ratio_of_defaults_for_sparse_serialization = 0.01;
INSERT INTO t_sparse_neg_zero_bf16 VALUES (-0.0), (1.0), (2.0);
SELECT hex(reinterpretAsFixedString(f)) FROM t_sparse_neg_zero_bf16 ORDER BY rowNumberInAllBlocks();
DROP TABLE t_sparse_neg_zero_bf16;

-- Float32
DROP TABLE IF EXISTS t_sparse_neg_zero_f32;
CREATE TABLE t_sparse_neg_zero_f32 (f Float32) Engine = MergeTree ORDER BY tuple() SETTINGS ratio_of_defaults_for_sparse_serialization = 0.01;
INSERT INTO t_sparse_neg_zero_f32 VALUES (-0.0), (1.0), (2.0);
SELECT hex(reinterpretAsFixedString(f)) FROM t_sparse_neg_zero_f32 ORDER BY rowNumberInAllBlocks();
DROP TABLE t_sparse_neg_zero_f32;

-- Float64
DROP TABLE IF EXISTS t_sparse_neg_zero_f64;
CREATE TABLE t_sparse_neg_zero_f64 (f Float64) Engine = MergeTree ORDER BY tuple() SETTINGS ratio_of_defaults_for_sparse_serialization = 0.01;
INSERT INTO t_sparse_neg_zero_f64 VALUES (-0.0), (1.0), (2.0);
SELECT hex(reinterpretAsFixedString(f)) FROM t_sparse_neg_zero_f64 ORDER BY rowNumberInAllBlocks();
DROP TABLE t_sparse_neg_zero_f64;

-- Verify that +0.0 is still treated as default (sparse-compressed)
DROP TABLE IF EXISTS t_sparse_pos_zero;
CREATE TABLE t_sparse_pos_zero (f Float64) Engine = MergeTree ORDER BY tuple() SETTINGS ratio_of_defaults_for_sparse_serialization = 0.01;
INSERT INTO t_sparse_pos_zero VALUES (0.0), (1.0), (2.0);
SELECT f FROM t_sparse_pos_zero ORDER BY rowNumberInAllBlocks();
DROP TABLE t_sparse_pos_zero;
