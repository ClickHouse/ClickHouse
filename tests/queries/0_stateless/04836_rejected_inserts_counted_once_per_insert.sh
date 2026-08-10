#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A plain INSERT fans out to `max_insert_threads` parallel sinks, and every sink evaluates the
# "too many parts" check while the insert chain is being built. One rejected INSERT must still be
# counted exactly once in the `RejectedInserts` profile event, not once per sink stream.

$CLICKHOUSE_CLIENT --query "
    CREATE TABLE t_rejected_inserts (x UInt64) ENGINE = MergeTree ORDER BY x
    SETTINGS parts_to_throw_insert = 1, parts_to_delay_insert = 0, max_avg_part_size_for_too_many_parts = 0;
"

# Brings the table to the throw threshold.
$CLICKHOUSE_CLIENT --async_insert 0 --query "INSERT INTO t_rejected_inserts VALUES (1)"

QUERY_ID="04836_${CLICKHOUSE_DATABASE}_rejected"

$CLICKHOUSE_CLIENT --query_id "$QUERY_ID" --async_insert 0 --max_insert_threads 16 \
    --query "INSERT INTO t_rejected_inserts VALUES (2)" 2>&1 | grep -o -m1 'TOO_MANY_PARTS'

$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"

$CLICKHOUSE_CLIENT --query "
    SELECT ProfileEvents['RejectedInserts']
    FROM system.query_log
    WHERE current_database = currentDatabase() AND query_id = '$QUERY_ID' AND type = 'ExceptionWhileProcessing'
    ORDER BY event_time_microseconds DESC
    LIMIT 1;
"
