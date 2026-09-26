#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
set -e

# A table attached while `table_readonly = 1` starts no background workers at all.
# Turning the setting back off must start them, otherwise nothing in the background
# ever runs again until the server restarts.
#
# A background mutation is the signal: it is picked up only by `background_operations_assignee`,
# whose holder does not exist for a table that was attached read-only.

$CLICKHOUSE_CLIENT --multiquery -q "
    DROP TABLE IF EXISTS readonly_toggle SYNC;
    CREATE TABLE readonly_toggle (k UInt64) ENGINE = MergeTree ORDER BY k
    SETTINGS table_readonly = 0;
    INSERT INTO readonly_toggle SELECT number FROM numbers(10);
    ALTER TABLE readonly_toggle MODIFY SETTING table_readonly = 1;
    DETACH TABLE readonly_toggle;
    ATTACH TABLE readonly_toggle;
"

# Still read-only after the attach: a mutation is rejected outright.
$CLICKHOUSE_CLIENT -q "ALTER TABLE readonly_toggle DELETE WHERE k = 0 SETTINGS mutations_sync = 0" 2>&1 \
    | grep -q -F 'TABLE_IS_PERMANENTLY_READ_ONLY' && echo 'rejected while readonly: 1'

$CLICKHOUSE_CLIENT -q "ALTER TABLE readonly_toggle MODIFY SETTING table_readonly = 0"

# The mutation is now accepted and must be executed in the background.
$CLICKHOUSE_CLIENT -q "ALTER TABLE readonly_toggle DELETE WHERE k = 0 SETTINGS mutations_sync = 0"

done_in_background=0
for _ in $(seq 1 600); do
    if [[ "$($CLICKHOUSE_CLIENT -q "SELECT is_done FROM system.mutations
                WHERE database = currentDatabase() AND table = 'readonly_toggle'
                ORDER BY create_time DESC LIMIT 1")" == "1" ]]; then
        done_in_background=1
        break
    fi
    sleep 0.1
done
echo "background mutation executed after toggle: $done_in_background"
echo "rows: $($CLICKHOUSE_CLIENT -q 'SELECT count() FROM readonly_toggle')"

$CLICKHOUSE_CLIENT -q "DROP TABLE readonly_toggle SYNC"
