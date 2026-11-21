-- Regression test for issue #23727:
-- `ALTER TABLE dst REPLACE PARTITION X FROM src` used to silently drop the destination
-- partition's data when the source had no parts in partition X. The minimal-row case
-- always slipped under the default `max_partition_size_to_drop = 50GiB` safety net.
--
-- We now refuse the operation by default and require users who actually depend on the
-- silent-clear semantics to opt in via `allow_replace_partition_from_empty_source = 1`.

DROP TABLE IF EXISTS t_replace_empty_src;
DROP TABLE IF EXISTS t_replace_empty_dst;

CREATE TABLE t_replace_empty_src (dt Date, val UInt64)
ENGINE = MergeTree PARTITION BY toYYYYMM(dt) ORDER BY dt;

CREATE TABLE t_replace_empty_dst (dt Date, val UInt64)
ENGINE = MergeTree PARTITION BY toYYYYMM(dt) ORDER BY dt;

INSERT INTO t_replace_empty_dst VALUES ('2023-01-15', 100), ('2023-01-20', 200);

SELECT 'default: REPLACE PARTITION FROM empty source is rejected';
ALTER TABLE t_replace_empty_dst REPLACE PARTITION 202301 FROM t_replace_empty_src; -- { serverError BAD_ARGUMENTS }

SELECT 'destination data preserved', count() FROM t_replace_empty_dst;

SELECT 'opt-in: legacy silent-drop is still available';
ALTER TABLE t_replace_empty_dst REPLACE PARTITION 202301 FROM t_replace_empty_src
    SETTINGS allow_replace_partition_from_empty_source = 1;

SELECT 'destination data cleared', count() FROM t_replace_empty_dst;

-- `compatibility` < 26.6 must also restore the legacy silent-drop (the setting keeps a 26.6 history
-- record with previous_value=true). Fails if the record's version or previous_value regresses.
INSERT INTO t_replace_empty_dst VALUES ('2023-01-15', 100), ('2023-01-20', 200);
ALTER TABLE t_replace_empty_dst REPLACE PARTITION 202301 FROM t_replace_empty_src
    SETTINGS compatibility = '26.5';

SELECT 'compatibility = 26.5 also restores legacy silent-drop', count() FROM t_replace_empty_dst;

-- Normal REPLACE with a non-empty source must still work (this never required the new setting).
INSERT INTO t_replace_empty_dst VALUES ('2023-01-15', 100), ('2023-01-20', 200);
INSERT INTO t_replace_empty_src VALUES ('2023-01-10', 999);

ALTER TABLE t_replace_empty_dst REPLACE PARTITION 202301 FROM t_replace_empty_src;
SELECT 'normal REPLACE swapped', val FROM t_replace_empty_dst ORDER BY dt;

-- ATTACH PARTITION FROM (replace=false) from an empty source has always been a safe no-op;
-- the new setting only governs REPLACE.
DROP TABLE t_replace_empty_src;
CREATE TABLE t_replace_empty_src (dt Date, val UInt64)
ENGINE = MergeTree PARTITION BY toYYYYMM(dt) ORDER BY dt;

ALTER TABLE t_replace_empty_dst ATTACH PARTITION 202301 FROM t_replace_empty_src;
SELECT 'attach from empty did not touch dst', val FROM t_replace_empty_dst ORDER BY dt;

-- A source partition that only holds parts with no rows is empty too (#124118). `MOVE PARTITION ... TO TABLE`
-- and a mutation that deletes every row leave such a part active until cleanup removes it, which
-- `remove_empty_parts = 0` prevents here.
DROP TABLE IF EXISTS t_replace_moved_src;
DROP TABLE IF EXISTS t_replace_moved_to;
DROP TABLE IF EXISTS t_replace_deleted_src;

CREATE TABLE t_replace_moved_src (dt Date, val UInt64)
ENGINE = MergeTree PARTITION BY toYYYYMM(dt) ORDER BY dt SETTINGS remove_empty_parts = 0;
CREATE TABLE t_replace_moved_to (dt Date, val UInt64)
ENGINE = MergeTree PARTITION BY toYYYYMM(dt) ORDER BY dt;

INSERT INTO t_replace_moved_src VALUES ('2023-01-10', 1);
ALTER TABLE t_replace_moved_src MOVE PARTITION 202301 TO TABLE t_replace_moved_to;

ALTER TABLE t_replace_empty_dst REPLACE PARTITION 202301 FROM t_replace_moved_src; -- { serverError BAD_ARGUMENTS }
SELECT 'source emptied by MOVE PARTITION is rejected, dst kept', val FROM t_replace_empty_dst ORDER BY dt;

CREATE TABLE t_replace_deleted_src (dt Date, val UInt64)
ENGINE = MergeTree PARTITION BY toYYYYMM(dt) ORDER BY dt SETTINGS remove_empty_parts = 0;

INSERT INTO t_replace_deleted_src VALUES ('2023-01-10', 1);
ALTER TABLE t_replace_deleted_src DELETE WHERE 1 SETTINGS mutations_sync = 2;
SELECT 'source with only a 0-row part after ALTER DELETE', count() FROM system.parts
WHERE database = currentDatabase() AND table = 't_replace_deleted_src' AND active AND rows = 0;

ALTER TABLE t_replace_empty_dst REPLACE PARTITION 202301 FROM t_replace_deleted_src; -- { serverError BAD_ARGUMENTS }
SELECT 'source emptied by ALTER DELETE is rejected, dst kept', val FROM t_replace_empty_dst ORDER BY dt;

-- A lightweight `DELETE` keeps the row in the part and only hides it, so `rows` stays 1.
DROP TABLE IF EXISTS t_replace_lwd_src;
CREATE TABLE t_replace_lwd_src (dt Date, val UInt64)
ENGINE = MergeTree PARTITION BY toYYYYMM(dt) ORDER BY dt SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO t_replace_lwd_src VALUES ('2023-01-10', 1);
DELETE FROM t_replace_lwd_src WHERE 1 SETTINGS lightweight_deletes_sync = 2;
SELECT 'source with only lightweight-deleted rows', count() FROM t_replace_lwd_src;

ALTER TABLE t_replace_empty_dst REPLACE PARTITION 202301 FROM t_replace_lwd_src; -- { serverError BAD_ARGUMENTS }
SELECT 'source emptied by a lightweight DELETE is rejected, dst kept', val FROM t_replace_empty_dst ORDER BY dt;

ALTER TABLE t_replace_empty_dst REPLACE PARTITION 202301 FROM t_replace_lwd_src
    SETTINGS allow_replace_partition_from_empty_source = 1;
SELECT 'opt-in still replaces from a lightweight-deleted source', count() FROM t_replace_empty_dst;

-- The cached live-row count can be stale: with `exclude_deleted_rows_for_part_size_in_merge = 0` the second
-- lightweight `DELETE` keeps the count of 1 left by the first one, although no row is live any more.
DROP TABLE IF EXISTS t_replace_stale_src;
CREATE TABLE t_replace_stale_src (dt Date, val UInt64)
ENGINE = MergeTree PARTITION BY toYYYYMM(dt) ORDER BY dt
SETTINGS min_bytes_for_wide_part = 0, exclude_deleted_rows_for_part_size_in_merge = 1;

INSERT INTO t_replace_stale_src VALUES ('2023-01-10', 1), ('2023-01-11', 2);
DELETE FROM t_replace_stale_src WHERE val = 1 SETTINGS lightweight_deletes_sync = 2;
ALTER TABLE t_replace_stale_src MODIFY SETTING exclude_deleted_rows_for_part_size_in_merge = 0;
DELETE FROM t_replace_stale_src WHERE 1 SETTINGS lightweight_deletes_sync = 2;

INSERT INTO t_replace_empty_dst VALUES ('2023-01-15', 100);
ALTER TABLE t_replace_empty_dst REPLACE PARTITION 202301 FROM t_replace_stale_src; -- { serverError BAD_ARGUMENTS }
SELECT 'source with a stale live-row count is rejected, dst kept', count() FROM t_replace_empty_dst;

DROP TABLE t_replace_stale_src;
DROP TABLE t_replace_lwd_src;
DROP TABLE t_replace_deleted_src;
DROP TABLE t_replace_moved_to;
DROP TABLE t_replace_moved_src;
DROP TABLE t_replace_empty_dst;
DROP TABLE t_replace_empty_src;

-- Same coverage on ReplicatedMergeTree.

DROP TABLE IF EXISTS t_replace_empty_src_r;
DROP TABLE IF EXISTS t_replace_empty_dst_r;

CREATE TABLE t_replace_empty_src_r (dt Date, val UInt64)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/04238/t_replace_empty_src_r', '1')
PARTITION BY toYYYYMM(dt) ORDER BY dt;

CREATE TABLE t_replace_empty_dst_r (dt Date, val UInt64)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/04238/t_replace_empty_dst_r', '1')
PARTITION BY toYYYYMM(dt) ORDER BY dt;

INSERT INTO t_replace_empty_dst_r VALUES ('2023-01-15', 100), ('2023-01-20', 200);

SELECT 'replicated default: REPLACE PARTITION FROM empty source is rejected';
ALTER TABLE t_replace_empty_dst_r REPLACE PARTITION 202301 FROM t_replace_empty_src_r; -- { serverError BAD_ARGUMENTS }

SELECT 'replicated destination data preserved', count() FROM t_replace_empty_dst_r;

SELECT 'replicated opt-in: legacy silent-drop still available';
ALTER TABLE t_replace_empty_dst_r REPLACE PARTITION 202301 FROM t_replace_empty_src_r
    SETTINGS allow_replace_partition_from_empty_source = 1;

SELECT 'replicated destination data cleared', count() FROM t_replace_empty_dst_r;

DROP TABLE IF EXISTS t_replace_deleted_src_r;

CREATE TABLE t_replace_deleted_src_r (dt Date, val UInt64)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/04238/t_replace_deleted_src_r', '1')
PARTITION BY toYYYYMM(dt) ORDER BY dt SETTINGS remove_empty_parts = 0;

INSERT INTO t_replace_deleted_src_r VALUES ('2023-01-10', 1);
ALTER TABLE t_replace_deleted_src_r DELETE WHERE 1 SETTINGS mutations_sync = 2;
SELECT 'replicated source with only a 0-row part after ALTER DELETE', count() FROM system.parts
WHERE database = currentDatabase() AND table = 't_replace_deleted_src_r' AND active AND rows = 0;

INSERT INTO t_replace_empty_dst_r VALUES ('2023-01-15', 100), ('2023-01-20', 200);
ALTER TABLE t_replace_empty_dst_r REPLACE PARTITION 202301 FROM t_replace_deleted_src_r; -- { serverError BAD_ARGUMENTS }
SELECT 'replicated source emptied by ALTER DELETE is rejected, dst kept', count() FROM t_replace_empty_dst_r;

DROP TABLE IF EXISTS t_replace_lwd_src_r;
CREATE TABLE t_replace_lwd_src_r (dt Date, val UInt64)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/04238/t_replace_lwd_src_r', '1')
PARTITION BY toYYYYMM(dt) ORDER BY dt SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO t_replace_lwd_src_r VALUES ('2023-01-10', 1);
DELETE FROM t_replace_lwd_src_r WHERE 1 SETTINGS lightweight_deletes_sync = 2;

ALTER TABLE t_replace_empty_dst_r REPLACE PARTITION 202301 FROM t_replace_lwd_src_r; -- { serverError BAD_ARGUMENTS }
SELECT 'replicated source emptied by a lightweight DELETE is rejected, dst kept', count() FROM t_replace_empty_dst_r;

DROP TABLE t_replace_lwd_src_r SYNC;
DROP TABLE t_replace_deleted_src_r SYNC;
DROP TABLE t_replace_empty_dst_r SYNC;
DROP TABLE t_replace_empty_src_r SYNC;
