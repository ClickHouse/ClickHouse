#!/usr/bin/env bash
# Tags: no-fasttest, long, no-random-settings
# no-fasttest: needs S3, and the test is slow (exercises throttling)
# no-random-settings: S3 prefetch settings affect read throughput; disabling prefetch can make
# the natural read rate drop close to the throttle limit, causing the throttler to never sleep

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
    INSERT INTO data_04103 SELECT * FROM numbers(2e5);
    GRANT SELECT, INSERT ON ${CLICKHOUSE_DATABASE}.data_04103 TO $READ_USER, $WRITE_USER;
"

write_query_id="04103_w_$CLICKHOUSE_DATABASE"
read_query_id="04103_r_$CLICKHOUSE_DATABASE"

# Writing/reading ~2e5*8 bytes with a 200 KB/s cap should each take ~8 seconds.
# They run in parallel (~8s wall-clock) because each user has its own throttler.
# The throttle limit is intentionally well below the natural S3 read rate observed on
# loaded TSAN parallel runners (~0.9 MB/s) so the throttler reliably enters its sleep
# path. With a 1 MB/s limit (the previous value), heavy contention occasionally let
# the natural rate drop near the limit, the throttler did not need to sleep, and the
# `UserThrottlerSleepMicroseconds` assertion flapped (issue #103422).
$CLICKHOUSE_CLIENT --user "$WRITE_USER" --query_id "$write_query_id" -q "
    INSERT INTO ${CLICKHOUSE_DATABASE}.data_04103 SELECT number+2e5 FROM numbers(2e5)
    SETTINGS max_network_bandwidth_for_user = 200000
" &
$CLICKHOUSE_CLIENT --user "$READ_USER" --query_id "$read_query_id" -q "
    SELECT * FROM ${CLICKHOUSE_DATABASE}.data_04103 WHERE key < 2e5 FORMAT Null
    SETTINGS max_network_bandwidth_for_user = 200000
" &
wait

# FIXME: some issues with query plan serialization
$CLICKHOUSE_CLIENT --serialize_query_plan=0 -m -q "
    SYSTEM FLUSH LOGS query_log;
    SELECT
        if(query_id = '$write_query_id', 'write', 'read') AS q,
        query_duration_ms >= 7e3,
        ProfileEvents['UserThrottlerBytes'] > 1.6e6,
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
