-- Tags: no-fasttest
-- Tag no-fasttest: Depends on AWS/MinIO paimon_all_types dataset

SET enable_time_time64_type = 1, session_timezone = 'UTC';

DROP TABLE IF EXISTS paimon_target_snapshot;

CREATE TABLE paimon_target_snapshot
ENGINE = PaimonS3(s3_conn, filename = 'paimon_all_types')
SETTINGS
    paimon_incremental_read = 1,
    paimon_target_snapshot_id = 1,
    paimon_keeper_path = '/clickhouse/tables/{uuid}',
    paimon_replica_name = '{replica}';

-- Targeted snapshot read should return the delta of snapshot 1 every time
SELECT count() FROM paimon_target_snapshot;
SELECT count() FROM paimon_target_snapshot;

DROP TABLE IF EXISTS paimon_target_snapshot;

