-- Tags: zookeeper, no-parallel-replicas, no-shared-merge-tree

-- `ReplicatedMergeTree::totalRows` honors `select_sequential_consistency` by
-- restricting active parts to those below the ZK max-added-block boundary. The
-- sparsity-aware trivial-count rewrite must consult the same part set so that
-- under sequential consistency it returns the quorum-acknowledged count, not the
-- local-only one.

SET enable_analyzer = 1;

DROP TABLE IF EXISTS t_sparse_seq_consistency_r1 SYNC;
DROP TABLE IF EXISTS t_sparse_seq_consistency_r2 SYNC;

CREATE TABLE t_sparse_seq_consistency_r1 (id UInt64, n UInt32)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test_04283/t', 'r1')
ORDER BY id
SETTINGS ratio_of_defaults_for_sparse_serialization = 0.5,
         compute_exact_num_defaults_for_sparse_columns = 1,
         serialization_info_version = 'with_types',
         min_bytes_for_wide_part = 0;

CREATE TABLE t_sparse_seq_consistency_r2 (id UInt64, n UInt32)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test_04283/t', 'r2')
ORDER BY id
SETTINGS ratio_of_defaults_for_sparse_serialization = 0.5,
         compute_exact_num_defaults_for_sparse_columns = 1,
         serialization_info_version = 'with_types',
         min_bytes_for_wide_part = 0;

-- First insert, quorum-confirmed by both replicas: 4000 default zeros + 1000 ones.
SET insert_quorum = 2, insert_quorum_parallel = 0;
INSERT INTO t_sparse_seq_consistency_r1
SELECT number, if(number < 4000, 0, 1)::UInt32 FROM numbers(5000)
SETTINGS optimize_on_insert = 0;

SYSTEM SYNC REPLICA t_sparse_seq_consistency_r2;
SYSTEM STOP FETCHES t_sparse_seq_consistency_r2;

-- Second insert. With FETCHES stopped on r2 the quorum is unsatisfiable, so the
-- insert times out (UNKNOWN_STATUS_OF_INSERT) but the part lands locally on r1.
-- After this, r1 has both parts locally; the second is not quorum-acknowledged.
SET insert_quorum_timeout = 100;
INSERT INTO t_sparse_seq_consistency_r1
SELECT number + 5000, if(number < 4000, 0, 1)::UInt32 FROM numbers(5000)
SETTINGS optimize_on_insert = 0; -- { serverError UNKNOWN_STATUS_OF_INSERT }

-- Without sequential consistency the rewrite counts both parts: 4000 + 4000.
SELECT 'seq0_rewrite', count() FROM t_sparse_seq_consistency_r1 WHERE n = 0
    SETTINGS optimize_trivial_count_with_sparsity_filter = 1,
             use_sparsity_info_for_pruning = 'off',
             select_sequential_consistency = 0;
SELECT 'seq0_scan',    count() FROM t_sparse_seq_consistency_r1 WHERE n = 0
    SETTINGS optimize_trivial_count_with_sparsity_filter = 0,
             use_sparsity_info_for_pruning = 'off',
             select_sequential_consistency = 0;

-- With sequential consistency both paths must see only the quorum-acknowledged
-- first part: 4000 defaults. Before the fix the rewrite returned 8000 because
-- `getColumnDefaultnessStats` iterated `getVisibleDataPartsVector` instead of
-- the ZK-filtered active parts list.
SELECT 'seq1_rewrite', count() FROM t_sparse_seq_consistency_r1 WHERE n = 0
    SETTINGS optimize_trivial_count_with_sparsity_filter = 1,
             use_sparsity_info_for_pruning = 'off',
             select_sequential_consistency = 1;
SELECT 'seq1_scan',    count() FROM t_sparse_seq_consistency_r1 WHERE n = 0
    SETTINGS optimize_trivial_count_with_sparsity_filter = 0,
             use_sparsity_info_for_pruning = 'off',
             select_sequential_consistency = 1;

SYSTEM START FETCHES t_sparse_seq_consistency_r2;
DROP TABLE t_sparse_seq_consistency_r1 SYNC;
DROP TABLE t_sparse_seq_consistency_r2 SYNC;
