-- Tags: no-fasttest

DROP TABLE IF EXISTS t_metadata_version SYNC;

CREATE TABLE t_metadata_version (id UInt64)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/02840_metadata_version', '1')
ORDER BY id
PARTITION BY id
SETTINGS min_bytes_for_full_part_storage = '1G', disable_detach_partition_for_zero_copy_replication = 0;

INSERT INTO t_metadata_version VALUES (1);

ALTER TABLE t_metadata_version DETACH PARTITION 1;
ALTER TABLE t_metadata_version ATTACH PARTITION 1;

DETACH TABLE t_metadata_version;
ATTACH TABLE t_metadata_version;

SYSTEM FLUSH LOGS text_log;

-- Bound to recent rows so the (event_date, event_time) PK prunes the scan; keep
-- max_rows_to_read = 0 so heavy-logging configs can't trip the read limit either way.
SELECT count() FROM system.text_log
    WHERE event_date >= yesterday() AND event_time >= now() - 600
        AND level = 'Warning'
        AND logger_name LIKE currentDatabase() || '.t_metadata_version%'
        AND message LIKE '%metadata version%'
SETTINGS max_rows_to_read = 0;

DROP TABLE IF EXISTS t_metadata_version SYNC;
