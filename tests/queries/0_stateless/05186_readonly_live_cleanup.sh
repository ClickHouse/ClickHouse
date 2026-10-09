#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -euo pipefail

${CLICKHOUSE_CLIENT} --multiquery --query "
CREATE TABLE readonly_live_cleanup (x UInt64) ENGINE = MergeTree ORDER BY x
SETTINGS old_parts_lifetime = 0, cleanup_delay_period = 1, max_cleanup_delay_period = 1,
    merge_tree_clear_old_parts_interval_seconds = 0;
SYSTEM STOP CLEANUP readonly_live_cleanup;
INSERT INTO readonly_live_cleanup VALUES (1);
INSERT INTO readonly_live_cleanup VALUES (2);
OPTIMIZE TABLE readonly_live_cleanup FINAL;
ALTER TABLE readonly_live_cleanup MODIFY SETTING table_readonly = 1;
SYSTEM START CLEANUP readonly_live_cleanup;
"

# Give the previously scheduled worker a cleanup interval after releasing the action lock.
sleep 2
${CLICKHOUSE_CLIENT} --query "
SELECT countIf(NOT active) = 2 FROM system.parts
WHERE database = currentDatabase() AND table = 'readonly_live_cleanup'"

${CLICKHOUSE_CLIENT} --query "ALTER TABLE readonly_live_cleanup MODIFY SETTING table_readonly = 0"
# Cleanup must resume when the table becomes writable again.
for _ in {1..100}; do
    remaining=$(${CLICKHOUSE_CLIENT} --query "SELECT countIf(NOT active) FROM system.parts WHERE database = currentDatabase() AND table = 'readonly_live_cleanup'")
    if [[ "$remaining" == 0 ]]; then
        echo resumed
        ${CLICKHOUSE_CLIENT} --query "DROP TABLE readonly_live_cleanup SYNC"
        exit 0
    fi
    sleep 0.1
done

echo 'Cleanup did not resume' >&2
exit 1
