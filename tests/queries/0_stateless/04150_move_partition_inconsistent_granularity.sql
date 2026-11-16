-- Tags: zookeeper, no-shared-merge-tree
-- no-shared-merge-tree: in Cloud/private CI ReplicatedMergeTree is substituted by
--   SharedMergeTree, whose partition-move code does not go through this
--   canReplacePartition granularity check, so the Replicated BAD_ARGUMENTS
--   assertions below do not hold under SMT. The plain MergeTree cases still cover
--   the fix on every configuration.
-- Regression test for STID 4063-3b45.
-- MOVE/REPLACE/ATTACH PARTITION between two tables with incompatible granularity
-- settings (one adaptive, one non-adaptive) used to throw LOGICAL_ERROR in three of
-- the four canReplacePartition call sites:
--   * src/Storages/StorageMergeTree.cpp        - MOVE PARTITION TO TABLE
--   * src/Storages/StorageReplicatedMergeTree.cpp - REPLACE PARTITION FROM
--   * src/Storages/StorageReplicatedMergeTree.cpp - MOVE PARTITION TO TABLE
-- This is a user-reachable condition (BuzzHouse hit it on 2026-05-01), so the
-- correct error code is BAD_ARGUMENTS, not LOGICAL_ERROR. The wrong code caused
-- a server-side abort in debug / sanitizer builds and a misleading internal-bug
-- exception in release builds.
--
-- All four call sites now throw BAD_ARGUMENTS. The fourth site (REPLACE PARTITION
-- on plain MergeTree, StorageMergeTree.cpp ~2601) was already BAD_ARGUMENTS before
-- this fix and is exercised here as well to lock in the behaviour.

DROP TABLE IF EXISTS src_nonadaptive SYNC;
DROP TABLE IF EXISTS dst_adaptive SYNC;
DROP TABLE IF EXISTS r_src_nonadaptive SYNC;
DROP TABLE IF EXISTS r_dst_adaptive SYNC;

-- ===== Plain MergeTree =====

CREATE TABLE src_nonadaptive (a UInt64, b UInt64)
ENGINE = MergeTree ORDER BY a
SETTINGS index_granularity_bytes = 0, enable_mixed_granularity_parts = 0;

CREATE TABLE dst_adaptive (a UInt64, b UInt64)
ENGINE = MergeTree ORDER BY a
SETTINGS index_granularity_bytes = 10485760, enable_mixed_granularity_parts = 0;

INSERT INTO src_nonadaptive SELECT number, number FROM numbers(1000);

-- Plain MergeTree: MOVE PARTITION (StorageMergeTree.cpp ~2762, was LOGICAL_ERROR)
ALTER TABLE src_nonadaptive MOVE PARTITION tuple() TO TABLE dst_adaptive; -- { serverError BAD_ARGUMENTS }

-- Plain MergeTree: REPLACE PARTITION (StorageMergeTree.cpp ~2601, was already BAD_ARGUMENTS)
ALTER TABLE dst_adaptive REPLACE PARTITION tuple() FROM src_nonadaptive; -- { serverError BAD_ARGUMENTS }

-- ===== Replicated MergeTree =====

CREATE TABLE r_src_nonadaptive (a UInt64, b UInt64)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/04150/r_src', 'r1')
ORDER BY a
SETTINGS index_granularity_bytes = 0, enable_mixed_granularity_parts = 0;

CREATE TABLE r_dst_adaptive (a UInt64, b UInt64)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/04150/r_dst', 'r1')
ORDER BY a
SETTINGS index_granularity_bytes = 10485760, enable_mixed_granularity_parts = 0;

INSERT INTO r_src_nonadaptive SELECT number, number FROM numbers(1000);

-- Replicated: REPLACE PARTITION FROM (StorageReplicatedMergeTree.cpp ~8992, was LOGICAL_ERROR)
ALTER TABLE r_dst_adaptive REPLACE PARTITION tuple() FROM r_src_nonadaptive; -- { serverError BAD_ARGUMENTS }

-- Replicated: MOVE PARTITION TO TABLE (StorageReplicatedMergeTree.cpp ~9287, was LOGICAL_ERROR)
ALTER TABLE r_src_nonadaptive MOVE PARTITION tuple() TO TABLE r_dst_adaptive; -- { serverError BAD_ARGUMENTS }

DROP TABLE src_nonadaptive SYNC;
DROP TABLE dst_adaptive SYNC;
DROP TABLE r_src_nonadaptive SYNC;
DROP TABLE r_dst_adaptive SYNC;
