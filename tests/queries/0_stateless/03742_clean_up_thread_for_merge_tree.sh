#!/usr/bin/env bash
# Tags: no-parallel
# Tag no-parallel: Fails due to failpoint intersection

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

on_exit() {
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT storage_merge_tree_background_schedule_merge_fail;"
}

trap on_exit EXIT

# Prepare
$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS m;

    CREATE TABLE m (
        i Int32
    ) ENGINE = MergeTree
    ORDER BY i
    SETTINGS old_parts_lifetime = 1, merge_tree_clear_old_parts_interval_seconds = 1, cleanup_delay_period = 1, cleanup_delay_period_random_add = 0, cleanup_thread_preferred_points_per_iteration = 0;

    SYSTEM ENABLE FAILPOINT storage_merge_tree_background_schedule_merge_fail;

    INSERT INTO m VALUES (1);
    INSERT INTO m VALUES (2);

    OPTIMIZE TABLE m FINAL;
"

function parts_count() {
    $CLICKHOUSE_CLIENT --query "SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 'm';"
}

# Wait up to 60 seconds until count = 1
ok=0
for _ in $(seq 1 60); do
    CNT=$(parts_count)
    if [ "$CNT" -eq 1 ]; then
        ok=1
        break
    fi

    sleep 1
done

if [ "$ok" -eq 1 ]; then
    echo "OK: parts count reached 1"
else
    echo "FAIL: parts count never reached 1 within 60 seconds"
fi

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS m;"

$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS m;
"
