-- Tags: zookeeper, no-shared-merge-tree
-- no-shared-merge-tree: in Cloud/private CI ReplicatedMergeTree is substituted by
--   SharedMergeTree, whose partition-move code does not go through this
--   canReplacePartition granularity check, so the Replicated BAD_ARGUMENTS
--   assertions below do not hold under SMT. The plain MergeTree cases still cover
--   the fix on every configuration.
-- Regression test for STID 4063-3b45 and STID 3484-4b1f (issue #117524).
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
DROP TABLE IF EXISTS src_g8, dst_g4, dst_g4_mixed, dst_g8, dst_g8_mixed, r_dst_g4096, src_g4, src_adaptive_g4 SYNC;

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
SETTINGS index_granularity = 8192, index_granularity_bytes = 0, enable_mixed_granularity_parts = 0;

CREATE TABLE r_dst_adaptive (a UInt64, b UInt64)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/04150/r_dst', 'r1')
ORDER BY a
SETTINGS index_granularity_bytes = 10485760, enable_mixed_granularity_parts = 0;

INSERT INTO r_src_nonadaptive SELECT number, number FROM numbers(1000);

-- Replicated: REPLACE PARTITION FROM (StorageReplicatedMergeTree.cpp ~8992, was LOGICAL_ERROR)
ALTER TABLE r_dst_adaptive REPLACE PARTITION tuple() FROM r_src_nonadaptive; -- { serverError BAD_ARGUMENTS }

-- Replicated: MOVE PARTITION TO TABLE (StorageReplicatedMergeTree.cpp ~9287, was LOGICAL_ERROR)
ALTER TABLE r_src_nonadaptive MOVE PARTITION tuple() TO TABLE r_dst_adaptive; -- { serverError BAD_ARGUMENTS }

-- ===== Non-adaptive source part, different index_granularity (issue #117524, STID 3484-4b1f) =====
-- A non-adaptive part stores no per-mark row counts, so the destination rebuilds them from its own
-- index_granularity. A mismatch made every mark map to the wrong row range.

CREATE TABLE src_g8 (a UInt64) ENGINE = MergeTree ORDER BY a
SETTINGS index_granularity = 8, index_granularity_bytes = 0, enable_mixed_granularity_parts = 0;

CREATE TABLE dst_g4 (a UInt64) ENGINE = MergeTree ORDER BY a
SETTINGS index_granularity = 4, index_granularity_bytes = 0, enable_mixed_granularity_parts = 0;

CREATE TABLE dst_g4_mixed (a UInt64) ENGINE = MergeTree ORDER BY a
SETTINGS index_granularity = 4, index_granularity_bytes = 10485760, enable_mixed_granularity_parts = 1;

CREATE TABLE dst_g8 (a UInt64) ENGINE = MergeTree ORDER BY a
SETTINGS index_granularity = 8, index_granularity_bytes = 0, enable_mixed_granularity_parts = 0;

CREATE TABLE dst_g8_mixed (a UInt64) ENGINE = MergeTree ORDER BY a
SETTINGS index_granularity = 8, index_granularity_bytes = 10485760, enable_mixed_granularity_parts = 1;

INSERT INTO src_g8 SELECT number FROM numbers(18);

-- A1 REPLACE into a non-adaptive table with a different index_granularity
ALTER TABLE dst_g4 REPLACE PARTITION tuple() FROM src_g8; -- { serverError BAD_ARGUMENTS }
-- A2 the same mismatch on the MOVE call site
ALTER TABLE src_g8 MOVE PARTITION tuple() TO TABLE dst_g4; -- { serverError BAD_ARGUMENTS }
-- A3 a destination that accepts mixed granularity still reinterprets a non-adaptive part
ALTER TABLE dst_g4_mixed REPLACE PARTITION tuple() FROM src_g8; -- { serverError BAD_ARGUMENTS }

-- A4 control: equal index_granularity is still accepted and reads back correctly
ALTER TABLE dst_g8 REPLACE PARTITION tuple() FROM src_g8;
SELECT count(), sum(a) FROM dst_g8;
-- A5 control: the same, into a destination that accepts mixed granularity
ALTER TABLE dst_g8_mixed REPLACE PARTITION tuple() FROM src_g8;
SELECT count(), sum(a) FROM dst_g8_mixed;

-- A6 Replicated: the same mismatch on the ReplicatedMergeTree call site
CREATE TABLE r_dst_g4096 (a UInt64, b UInt64)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/04150/r_dst_g4096', 'r1')
ORDER BY a
SETTINGS index_granularity = 4096, index_granularity_bytes = 0, enable_mixed_granularity_parts = 0;

ALTER TABLE r_dst_g4096 REPLACE PARTITION tuple() FROM r_src_nonadaptive; -- { serverError BAD_ARGUMENTS }

CREATE TABLE src_g4 (a UInt64) ENGINE = MergeTree ORDER BY a
SETTINGS index_granularity = 4, index_granularity_bytes = 0, enable_mixed_granularity_parts = 0;

INSERT INTO src_g4 SELECT number FROM numbers(18);

-- A7 the reversed direction, on ATTACH PARTITION FROM: a smaller source index_granularity is
-- rejected too, so a one-sided comparison would not satisfy this arm
ALTER TABLE dst_g8 ATTACH PARTITION tuple() FROM src_g4; -- { serverError BAD_ARGUMENTS }

CREATE TABLE src_adaptive_g4 (a UInt64) ENGINE = MergeTree ORDER BY a
SETTINGS index_granularity = 4, index_granularity_bytes = 10485760, enable_mixed_granularity_parts = 1,
         min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO src_adaptive_g4 SELECT number + 100 FROM numbers(18);

-- A8 control: an adaptive part carries a row count per mark, so the destination's index_granularity
-- does not enter its interpretation and a different value is still accepted. The source is pinned to
-- the Wide format because that is the format whose granularity load path holds the defect.
ALTER TABLE dst_g8_mixed REPLACE PARTITION tuple() FROM src_adaptive_g4;
SELECT count(), sum(a) FROM dst_g8_mixed;
SELECT count() FROM dst_g8_mixed WHERE a >= 109;

DROP TABLE src_nonadaptive SYNC;
DROP TABLE dst_adaptive SYNC;
DROP TABLE r_src_nonadaptive SYNC;
DROP TABLE r_dst_adaptive SYNC;
DROP TABLE src_g8 SYNC;
DROP TABLE dst_g4 SYNC;
DROP TABLE dst_g4_mixed SYNC;
DROP TABLE dst_g8 SYNC;
DROP TABLE dst_g8_mixed SYNC;
DROP TABLE r_dst_g4096 SYNC;
DROP TABLE src_g4 SYNC;
DROP TABLE src_adaptive_g4 SYNC;
