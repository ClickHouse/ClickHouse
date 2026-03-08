-- Tags: no-fasttest
-- Tag no-fasttest: Depends on AWS/MinIO paimon_all_types dataset

SET enable_time_time64_type = 1, session_timezone = 'UTC';

DROP TABLE IF EXISTS paimon_inc_read;

CREATE TABLE paimon_inc_read
ENGINE = PaimonS3(s3_conn, filename = 'paimon_all_types')
SETTINGS
    paimon_incremental_read = 1,
    paimon_keeper_path = '/clickhouse/tables/{uuid}',
    paimon_replica_name = '{replica}';

-- First run: initial delta (latest snapshot delta)
SELECT count() FROM paimon_inc_read;

-- Second run: no new snapshot, should be zero
SELECT count() FROM paimon_inc_read;

DROP TABLE IF EXISTS paimon_inc_read;

