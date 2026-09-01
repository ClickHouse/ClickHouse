#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A predicate on `uuid` must be pushed down into `system.tables` to prefilter the list of
# tables before the source materializes their rows. The push-down samples in
# `StorageSystemTables` used to declare `uuid` as `String` while the real column is `UUID`;
# with the exact-type check in `splitFilterDagForAllowedInputs`, such a lying sample would
# silently disable the prefilter. The `SelectedRows` profile event distinguishes the two:
# 1 with the prefilter, 3 (every table in the database) without.
# https://github.com/ClickHouse/ClickHouse/issues/113982

$CLICKHOUSE_CLIENT -q "
    CREATE TABLE t1_04831 (x UInt8) ENGINE = MergeTree ORDER BY x;
    CREATE TABLE t2_04831 (x UInt8) ENGINE = MergeTree ORDER BY x;
    CREATE TABLE t3_04831 (x UInt8) ENGINE = MergeTree ORDER BY x;
"

uuid=$($CLICKHOUSE_CLIENT -q "SELECT uuid FROM system.tables WHERE database = currentDatabase() AND name = 't1_04831'")

log_comment="04831_filter_pushdown_uuid_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT --log_comment "$log_comment" -q "
    SELECT name FROM system.tables WHERE database = currentDatabase() AND uuid = '$uuid';
"

$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"
$CLICKHOUSE_CLIENT -q "
    SELECT ProfileEvents['SelectedRows']
    FROM system.query_log
    WHERE current_database = currentDatabase() AND log_comment = '$log_comment' AND type = 'QueryFinish'
    ORDER BY event_time_microseconds DESC
    LIMIT 1;
"

$CLICKHOUSE_CLIENT -q "
    DROP TABLE t1_04831;
    DROP TABLE t2_04831;
    DROP TABLE t3_04831;
"
