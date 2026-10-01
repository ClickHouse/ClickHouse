-- Tags: zookeeper, no-shared-merge-tree, no-replicated-database
-- no-shared-merge-tree: the tables must be ReplicatedMergeTree with a ZooKeeper path to fetch from
-- no-replicated-database: FETCH ... FROM needs the source table's literal ZooKeeper path, which a Replicated database rewrites

DROP TABLE IF EXISTS r_fsrc_g8, r_fsrc_g4, r_fsrc_adaptive_g4, fsrc_nonadaptive_g8, r_fsrc_mixed_g8, r_fdst_g4, r_fdst_g8_mixed, dst_g4, restored_g8, fsrc_nonadaptive9_g8, r_fsrc_mixed9_g8, r_fdst_g6, src_s_g8, dst_s_g4 SYNC;

-- ===== FETCH PARTITION / FETCH PART from a table with another index_granularity (issue #123227) =====

CREATE TABLE r_fsrc_g8 (a UInt64)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/05315/r_fsrc_g8', 'r1') ORDER BY a
SETTINGS index_granularity = 8, index_granularity_bytes = 0, enable_mixed_granularity_parts = 0;
SYSTEM STOP MERGES r_fsrc_g8;
-- F2 fetches all_0_0_0 by name, and an insert retried after a Keeper fault takes the next block number
INSERT INTO r_fsrc_g8 SETTINGS insert_keeper_fault_injection_probability = 0 SELECT number FROM numbers(18);

CREATE TABLE r_fsrc_g4 (a UInt64)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/05315/r_fsrc_g4', 'r1') ORDER BY a
SETTINGS index_granularity = 4, index_granularity_bytes = 0, enable_mixed_granularity_parts = 0;
INSERT INTO r_fsrc_g4 SELECT number FROM numbers(18);

CREATE TABLE r_fsrc_adaptive_g4 (a UInt64)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/05315/r_fsrc_adaptive_g4', 'r1') ORDER BY a
SETTINGS index_granularity = 4, index_granularity_bytes = 10485760, enable_mixed_granularity_parts = 1;
INSERT INTO r_fsrc_adaptive_g4 SELECT number + 100 FROM numbers(18);

CREATE TABLE fsrc_nonadaptive_g8 (a UInt64) ENGINE = MergeTree ORDER BY a
SETTINGS index_granularity = 8, index_granularity_bytes = 0, enable_mixed_granularity_parts = 0,
         ratio_of_defaults_for_sparse_serialization = 1;
INSERT INTO fsrc_nonadaptive_g8 SELECT number FROM numbers(18);
OPTIMIZE TABLE fsrc_nonadaptive_g8 FINAL;

-- An adaptive table that holds a non-adaptive part (admitted because the index_granularity is equal)
CREATE TABLE r_fsrc_mixed_g8 (a UInt64)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/05315/r_fsrc_mixed_g8', 'r1') ORDER BY a
SETTINGS index_granularity = 8, index_granularity_bytes = 10485760, enable_mixed_granularity_parts = 1;
SYSTEM STOP MERGES r_fsrc_mixed_g8;
ALTER TABLE r_fsrc_mixed_g8 ATTACH PARTITION tuple() FROM fsrc_nonadaptive_g8;

CREATE TABLE r_fdst_g4 (a UInt64)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/05315/r_fdst_g4', 'r1') ORDER BY a
SETTINGS index_granularity = 4, index_granularity_bytes = 0, enable_mixed_granularity_parts = 0;

CREATE TABLE r_fdst_g8_mixed (a UInt64)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/05315/r_fdst_g8_mixed', 'r1') ORDER BY a
SETTINGS index_granularity = 8, index_granularity_bytes = 10485760, enable_mixed_granularity_parts = 1;

-- F1 FETCH PARTITION from a non-adaptive table with a larger index_granularity
ALTER TABLE r_fdst_g4 FETCH PARTITION tuple() FROM '/clickhouse/tables/{database}/05315/r_fsrc_g8'; -- { serverError BAD_ARGUMENTS }
-- F2 the same on FETCH PART
ALTER TABLE r_fdst_g4 FETCH PART 'all_0_0_0' FROM '/clickhouse/tables/{database}/05315/r_fsrc_g8'; -- { serverError BAD_ARGUMENTS }
-- F3 a smaller source index_granularity, into a destination that accepts mixed granularity
ALTER TABLE r_fdst_g8_mixed FETCH PARTITION tuple() FROM '/clickhouse/tables/{database}/05315/r_fsrc_g4'; -- { serverError BAD_ARGUMENTS }
-- nothing was downloaded into detached/
SELECT count() FROM system.detached_parts WHERE database = currentDatabase() AND table IN ('r_fdst_g4', 'r_fdst_g8_mixed');

-- F4 control: equal index_granularity is still fetched and reads back correctly
ALTER TABLE r_fdst_g8_mixed FETCH PARTITION tuple() FROM '/clickhouse/tables/{database}/05315/r_fsrc_g8';
ALTER TABLE r_fdst_g8_mixed ATTACH PARTITION tuple();
SELECT count(), sum(a) FROM r_fdst_g8_mixed;
SELECT count() FROM r_fdst_g8_mixed WHERE a >= 9;

-- F5 control: an adaptive source with another index_granularity is still fetched
ALTER TABLE r_fdst_g8_mixed FETCH PARTITION tuple() FROM '/clickhouse/tables/{database}/05315/r_fsrc_adaptive_g4';
ALTER TABLE r_fdst_g8_mixed ATTACH PARTITION tuple();
SELECT count(), sum(a) FROM r_fdst_g8_mixed;
SELECT count() FROM r_fdst_g8_mixed WHERE a >= 109;

-- F6 a non-adaptive part inside an adaptive source is rejected when it is loaded, and is not left in detached/
ALTER TABLE r_fdst_g4 FETCH PARTITION tuple() FROM '/clickhouse/tables/{database}/05315/r_fsrc_mixed_g8'; -- { serverError BAD_SIZE_OF_FILE_IN_DATA_PART }
SELECT count() FROM system.detached_parts WHERE database = currentDatabase() AND table = 'r_fdst_g4';

-- F7 the same with a row count that fits the destination's marks (9 rows written with 8, read with 6)
CREATE TABLE fsrc_nonadaptive9_g8 (a UInt64) ENGINE = MergeTree ORDER BY a
SETTINGS index_granularity = 8, index_granularity_bytes = 0, enable_mixed_granularity_parts = 0;
INSERT INTO fsrc_nonadaptive9_g8 SELECT number FROM numbers(9);
OPTIMIZE TABLE fsrc_nonadaptive9_g8 FINAL;
CREATE TABLE r_fsrc_mixed9_g8 (a UInt64)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/05315/r_fsrc_mixed9_g8', 'r1') ORDER BY a
SETTINGS index_granularity = 8, index_granularity_bytes = 10485760, enable_mixed_granularity_parts = 1;
SYSTEM STOP MERGES r_fsrc_mixed9_g8;
ALTER TABLE r_fsrc_mixed9_g8 ATTACH PARTITION tuple() FROM fsrc_nonadaptive9_g8;
CREATE TABLE r_fdst_g6 (a UInt64)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/05315/r_fdst_g6', 'r1') ORDER BY a
SETTINGS index_granularity = 6, index_granularity_bytes = 0, enable_mixed_granularity_parts = 0;
ALTER TABLE r_fdst_g6 FETCH PARTITION tuple() FROM '/clickhouse/tables/{database}/05315/r_fsrc_mixed9_g8'; -- { serverError BAD_ARGUMENTS }
SELECT count() FROM system.detached_parts WHERE database = currentDatabase() AND table = 'r_fdst_g6';

-- ===== RESTORE into a table with another index_granularity =====
CREATE TABLE dst_g4 (a UInt64) ENGINE = MergeTree ORDER BY a
SETTINGS index_granularity = 4, index_granularity_bytes = 0, enable_mixed_granularity_parts = 0;
BACKUP TABLE fsrc_nonadaptive_g8 TO Memory('05315_backup') FORMAT Null;
-- R1 a non-adaptive part restored into a table with another index_granularity is rejected
RESTORE TABLE fsrc_nonadaptive_g8 AS dst_g4 FROM Memory('05315_backup') SETTINGS allow_different_table_def = 1; -- { serverError BACKUP_DAMAGED }
SELECT count() FROM dst_g4;
-- R2 control: the same backup restores into a table created from its own definition
RESTORE TABLE fsrc_nonadaptive_g8 AS restored_g8 FROM Memory('05315_backup') FORMAT Null;
SELECT count(), sum(a) FROM restored_g8;
SELECT count() FROM restored_g8 WHERE a >= 9;
-- R3 a part without numeric columns is rejected too
CREATE TABLE src_s_g8 (s String) ENGINE = MergeTree ORDER BY s
SETTINGS index_granularity = 8, index_granularity_bytes = 0, enable_mixed_granularity_parts = 0,
         enable_block_number_column = 0, enable_block_offset_column = 0;
INSERT INTO src_s_g8 SELECT toString(number) FROM numbers(18);
OPTIMIZE TABLE src_s_g8 FINAL;
CREATE TABLE dst_s_g4 (s String) ENGINE = MergeTree ORDER BY s
SETTINGS index_granularity = 4, index_granularity_bytes = 0, enable_mixed_granularity_parts = 0;
BACKUP TABLE src_s_g8 TO Memory('05315_backup_s') FORMAT Null;
RESTORE TABLE src_s_g8 AS dst_s_g4 FROM Memory('05315_backup_s') SETTINGS allow_different_table_def = 1; -- { serverError BACKUP_DAMAGED }
SELECT count() FROM dst_s_g4;

DROP TABLE r_fsrc_g8 SYNC;
DROP TABLE r_fsrc_g4 SYNC;
DROP TABLE r_fsrc_adaptive_g4 SYNC;
DROP TABLE fsrc_nonadaptive_g8 SYNC;
DROP TABLE r_fsrc_mixed_g8 SYNC;
DROP TABLE r_fdst_g4 SYNC;
DROP TABLE r_fdst_g8_mixed SYNC;
DROP TABLE dst_g4 SYNC;
DROP TABLE restored_g8 SYNC;
DROP TABLE fsrc_nonadaptive9_g8 SYNC;
DROP TABLE r_fsrc_mixed9_g8 SYNC;
DROP TABLE r_fdst_g6 SYNC;
DROP TABLE src_s_g8 SYNC;
DROP TABLE dst_s_g4 SYNC;
