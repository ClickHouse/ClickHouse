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

-- `compatibility` < 26.6 must also restore the legacy silent-drop (SettingsChangesHistory keeps the
-- 26.6-bucket entry with previous_value=true). Fails if the entry's bucket or previous_value regresses.
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

DROP TABLE t_replace_empty_dst_r SYNC;
DROP TABLE t_replace_empty_src_r SYNC;
