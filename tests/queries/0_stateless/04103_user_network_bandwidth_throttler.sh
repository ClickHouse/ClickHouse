#!/usr/bin/env bash
# Tags: no-fasttest, long
# no-fasttest: needs S3, and the test is slow (exercises throttling)

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Separate users for INSERT and SELECT: each gets its own
# `ProcessListForUser::user_throttler`, so the two queries can run in parallel
# without sharing the same user-level token bucket.
READ_USER="u_04103_r_$CLICKHOUSE_DATABASE"
WRITE_USER="u_04103_w_$CLICKHOUSE_DATABASE"
$CLICKHOUSE_CLIENT --format Null -q "CREATE USER $READ_USER, $WRITE_USER"

$CLICKHOUSE_CLIENT -m -q "
    DROP TABLE IF EXISTS data_04103;
    CREATE TABLE data_04103 (key UInt64 CODEC(NONE)) ENGINE=MergeTree() ORDER BY key
        SETTINGS min_bytes_for_wide_part=1e9, disk='s3_disk';
    INSERT INTO data_04103 SELECT * FROM numbers(1e6);
    GRANT SELECT, INSERT ON ${CLICKHOUSE_DATABASE}.data_04103 TO $READ_USER, $WRITE_USER;
"

write_query_id="04103_w_$CLICKHOUSE_DATABASE"
read_query_id="04103_r_$CLICKHOUSE_DATABASE"

# Writing/reading ~1e6*8 bytes with a 1M/s cap should each take ~7 seconds.
# They run in parallel (~7s wall-clock) because each user has its own throttler.
$CLICKHOUSE_CLIENT --user "$WRITE_USER" --query_id "$write_query_id" -q "
    INSERT INTO ${CLICKHOUSE_DATABASE}.data_04103 SELECT number+1e6 FROM numbers(1e6)
    SETTINGS max_network_bandwidth_for_user = 1000000
" &
$CLICKHOUSE_CLIENT --user "$READ_USER" --query_id "$read_query_id" -q "
    SELECT * FROM ${CLICKHOUSE_DATABASE}.data_04103 WHERE key < 1e6 FORMAT Null
    SETTINGS max_network_bandwidth_for_user = 1000000
" &
wait

# FIXME: some issues with query plan serialization
$CLICKHOUSE_CLIENT --serialize_query_plan=0 -m -q "
    SYSTEM FLUSH LOGS query_log;
    SELECT
        if(query_id = '$write_query_id', 'write', 'read') AS q,
        query_duration_ms >= 7e3,
        ProfileEvents['UserThrottlerBytes'] > 8e6,
        ProfileEvents['UserThrottlerSleepMicroseconds'] > 7e6 * 0.5
    FROM system.query_log
    WHERE event_date >= yesterday() AND event_time >= now() - 600
      AND current_database = '$CLICKHOUSE_DATABASE'
      AND query_id IN ('$write_query_id', '$read_query_id') AND type != 'QueryStart'
    ORDER BY q
"

$CLICKHOUSE_CLIENT --format Null -q "
    DROP TABLE data_04103;
    DROP USER $READ_USER, $WRITE_USER;
"
