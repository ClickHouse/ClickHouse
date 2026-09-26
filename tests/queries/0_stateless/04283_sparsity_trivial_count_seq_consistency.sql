-- Tags: zookeeper, no-parallel-replicas, no-shared-merge-tree, no-old-analyzer
-- no-old-analyzer: Not supported

-- `ReplicatedMergeTree::totalRows` honors `select_sequential_consistency` by
-- restricting active parts to those below the ZK max-added-block boundary. The
-- sparsity-aware trivial-count rewrite must consult the same part set so that
-- under sequential consistency it returns the quorum-acknowledged count, not the
-- local-only one.

SET optimize_trivial_count_query = 1, insert_keeper_fault_injection_probability = 0;

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

-- First insert without quorum, then sync r2 so both replicas hold the part
-- (4000 default zeros + 1000 ones). The earlier `insert_quorum = 2,
-- insert_quorum_parallel = 0` design here was flaky under CI: randomised
-- `max_insert_threads > 1` spawns multiple `ReplicatedMergeTreeSink`
-- instances, and the precondition check of the second sink races against
-- the first sink's `/quorum/status`, throwing
-- `UNSATISFIED_QUORUM_FOR_PREVIOUS_WRITE`.
INSERT INTO t_sparse_seq_consistency_r1
SELECT number, if(number < 4000, 0, 1)::UInt32 FROM numbers(5000)
SETTINGS optimize_on_insert = 0;

SYSTEM SYNC REPLICA t_sparse_seq_consistency_r2;
SYSTEM STOP FETCHES t_sparse_seq_consistency_r2;

-- Second insert with quorum=2 and fetches stopped on r2: the quorum is
-- unsatisfiable so the insert errors out, but the part lands locally on r1
-- and `/quorum/status` is left set for it. `getMaxAddedBlocks` then clamps
-- the per-partition max block to `pending_part.max_block - 1`, which equals
-- the first part's block, so `select_sequential_consistency = 1` sees only
-- the first part.
SET insert_quorum = 2, insert_quorum_parallel = 0, insert_quorum_timeout = 100;
INSERT INTO t_sparse_seq_consistency_r1
SELECT number + 5000, if(number < 4000, 0, 1)::UInt32 FROM numbers(5000)
SETTINGS optimize_on_insert = 0; -- { serverError UNKNOWN_STATUS_OF_INSERT,UNSATISFIED_QUORUM_FOR_PREVIOUS_WRITE }

-- Without sequential consistency the rewrite counts both parts: 4000 + 4000.
SELECT 'seq0_rewrite', count() FROM t_sparse_seq_consistency_r1 WHERE n = 0
    SETTINGS optimize_trivial_count_with_sparsity_filter = 1,
             select_sequential_consistency = 0;
SELECT 'seq0_scan',    count() FROM t_sparse_seq_consistency_r1 WHERE n = 0
    SETTINGS optimize_trivial_count_with_sparsity_filter = 0,
             select_sequential_consistency = 0;

-- With sequential consistency both paths must see only the first part: 4000
-- defaults. Before the fix the rewrite returned 8000 because
-- `getColumnDefaultnessStats` iterated `getVisibleDataPartsVector` instead of
-- the ZK-filtered active parts list.
SELECT 'seq1_rewrite', count() FROM t_sparse_seq_consistency_r1 WHERE n = 0
    SETTINGS optimize_trivial_count_with_sparsity_filter = 1,
             select_sequential_consistency = 1;
SELECT 'seq1_scan',    count() FROM t_sparse_seq_consistency_r1 WHERE n = 0
    SETTINGS optimize_trivial_count_with_sparsity_filter = 0,
             select_sequential_consistency = 1;

SYSTEM START FETCHES t_sparse_seq_consistency_r2;
DROP TABLE t_sparse_seq_consistency_r1 SYNC;
DROP TABLE t_sparse_seq_consistency_r2 SYNC;
