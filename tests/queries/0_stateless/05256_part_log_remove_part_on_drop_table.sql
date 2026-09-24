-- DROP TABLE removes every part of the table in dropAllData(), which bypasses removePartsFinally().
-- It must still write one RemovePart event per part, as merges, DROP PARTITION and TRUNCATE do.

DROP TABLE IF EXISTS t_part_log_drop SYNC;
DROP TABLE IF EXISTS t_part_log_drop_replicated SYNC;

CREATE TABLE t_part_log_drop (x UInt64) ENGINE = MergeTree ORDER BY x;
SYSTEM STOP MERGES t_part_log_drop;
INSERT INTO t_part_log_drop VALUES (1);
INSERT INTO t_part_log_drop VALUES (2);
DROP TABLE t_part_log_drop SYNC;

CREATE TABLE t_part_log_drop_replicated (x UInt64)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_part_log_drop_replicated', 'r1') ORDER BY x;
SYSTEM STOP MERGES t_part_log_drop_replicated;
INSERT INTO t_part_log_drop_replicated VALUES (1);
INSERT INTO t_part_log_drop_replicated VALUES (2);
DROP TABLE t_part_log_drop_replicated SYNC;

SYSTEM FLUSH LOGS part_log;

SELECT table, event_type, count()
FROM system.part_log
WHERE database = currentDatabase() AND table IN ('t_part_log_drop', 't_part_log_drop_replicated')
GROUP BY table, event_type
ORDER BY table, event_type;
