-- Tags: zookeeper
-- Test: has_lightweight_delete flag recovery on ReplicatedMergeTree after merge.

DROP TABLE IF EXISTS t_lwd_rmt_flag SYNC;

CREATE TABLE t_lwd_rmt_flag (a UInt32, b String)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_lwd_rmt_flag', '1')
    ORDER BY a
    SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO t_lwd_rmt_flag SELECT number, toString(number) FROM numbers(1000);
OPTIMIZE TABLE t_lwd_rmt_flag FINAL;

-- S0: no LWD flag
SELECT 's0_no_flag:', count() = 0
FROM system.parts
WHERE table = 't_lwd_rmt_flag' AND database = currentDatabase() AND active AND has_lightweight_delete;

SET lightweight_deletes_sync = 2;
DELETE FROM t_lwd_rmt_flag WHERE a < 100;

-- S1: flag is set
SELECT 's1_has_flag:', count() > 0
FROM system.parts
WHERE table = 't_lwd_rmt_flag' AND database = currentDatabase() AND active AND has_lightweight_delete;

SELECT 's1_count:', count() = 900 FROM t_lwd_rmt_flag;

-- Merge and sync
OPTIMIZE TABLE t_lwd_rmt_flag FINAL;
SYSTEM SYNC REPLICA t_lwd_rmt_flag;

-- S2: flag must be cleared after merge
SELECT 's2_no_flag:', count() = 0
FROM system.parts
WHERE table = 't_lwd_rmt_flag' AND database = currentDatabase() AND active AND has_lightweight_delete;

SELECT 's2_count:', count() = 900 FROM t_lwd_rmt_flag;

DROP TABLE t_lwd_rmt_flag SYNC;
