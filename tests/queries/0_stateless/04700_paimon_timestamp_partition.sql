-- Tags: no-fasttest
-- Tag no-fasttest: Depends on AWS

SET session_timezone = 'UTC';

-- A Paimon table partitioned by TIMESTAMP(6) - the precision of a bare Paimon TIMESTAMP and of every
-- Spark TIMESTAMP, so this is the table from issue #112768. Reading it used to fail with
-- "scale 6 is not supported, only support scale <= 3".
DESC paimonS3(s3_conn, filename='paimon_timestamp_partition/ts6');
SELECT id, ts FROM paimonS3(s3_conn, filename='paimon_timestamp_partition/ts6') ORDER BY id;

SELECT '=== ts6 pruning ===';

-- Partition pruning compares the decoded partition value against the query range, so a value at the
-- wrong scale silently drops rows instead of failing.
SELECT id, ts FROM paimonS3(s3_conn, filename='paimon_timestamp_partition/ts6')
WHERE ts >= '2025-07-31 16:40:00.000456' AND ts <= '2025-07-31 16:40:00.123456'
ORDER BY id SETTINGS use_paimon_partition_pruning = 1;

SELECT '=== ts9 ===';

-- TIMESTAMP(9), covering every branch of the partition directory name Paimon writes: no fraction with
-- a zero second, no fraction with a non-zero second, and 3, 6 and 9 fractional digits. Row 6 is
-- before the epoch, where the millisecond is negative while nanoOfMillisecond counts forward.
DESC paimonS3(s3_conn, filename='paimon_timestamp_partition/ts9');
SELECT id, ts FROM paimonS3(s3_conn, filename='paimon_timestamp_partition/ts9') ORDER BY id;

SELECT '=== ts9 pruning ===';

-- The bound falls between the microsecond truncation of row 5 and its actual value, so the row
-- survives pruning only if the partition value keeps all nine digits.
SELECT id, ts FROM paimonS3(s3_conn, filename='paimon_timestamp_partition/ts9')
WHERE ts > '2025-07-31 16:40:00.123456500'
ORDER BY id SETTINGS use_paimon_partition_pruning = 1;

SELECT '=== ltz ===';

-- A TIMESTAMP(0) partition key uses the compact encoding below precision 3. A
-- TIMESTAMP WITH LOCAL TIME ZONE one is named by Paimon from the raw epoch millisecond with no time
-- zone applied, so a non-UTC session must still find the directory - reading in Asia/Shanghai is what
-- distinguishes that, under UTC it would pass either way.
DESC paimonS3(s3_conn, filename='paimon_timestamp_partition/ltz');

SET session_timezone = 'Asia/Shanghai';
SELECT id, ts0, tsltz FROM paimonS3(s3_conn, filename='paimon_timestamp_partition/ltz') ORDER BY id;

-- Exercise partition pruning for high-precision `TIMESTAMP WITH LOCAL TIME ZONE` while the session
-- uses a non-UTC time zone.
SELECT id, tsltz FROM paimonS3(s3_conn, filename='paimon_timestamp_partition/ltz')
WHERE tsltz = '2025-07-31 08:30:00.123456'
ORDER BY id SETTINGS use_paimon_partition_pruning = 1;

SET session_timezone = 'UTC';
SELECT id, ts0, tsltz FROM paimonS3(s3_conn, filename='paimon_timestamp_partition/ltz') ORDER BY id;
