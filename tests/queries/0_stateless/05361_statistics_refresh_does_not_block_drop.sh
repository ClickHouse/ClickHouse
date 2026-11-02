#!/usr/bin/env bash
# Tags: no-fasttest, no-shared-merge-tree, no-replicated-database
# Tag no-fasttest: requires S3
# Tag no-shared-merge-tree, no-replicated-database: the tables use their own ad hoc S3 disks

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Each table has its own disk, which serves 2 GET requests per second. A statistics refresh reads each of the 5
# statistics of a part with its own request, so a refresh of 24 parts takes at least 60 seconds. Neither ALTER of
# `refresh_statistics_interval` nor DROP may wait for a refresh in flight.
# HEAD requests count against that limit too, so the disk skips its access check (done under a server-wide lock) and
# the inserts disable `s3_check_objects_after_upload` (enabled in the test profile).

function create_table()
{
    $CLICKHOUSE_CLIENT -q "
        CREATE TABLE $1 (k UInt64, v1 UInt64 STATISTICS(basic), v2 UInt64 STATISTICS(basic), v3 UInt64 STATISTICS(basic),
            v4 UInt64 STATISTICS(basic), v5 UInt64 STATISTICS(basic)) ENGINE = MergeTree ORDER BY k
        SETTINGS refresh_statistics_interval = 0, auto_statistics_types = '',
            disk = disk(name = '${CLICKHOUSE_TEST_UNIQUE_NAME}_$1', type = object_storage, object_storage_type = s3,
                metadata_type = local, endpoint = 'http://localhost:11111/test/${CLICKHOUSE_TEST_UNIQUE_NAME}/$1/',
                access_key_id = clickhouse, secret_access_key = clickhouse, s3_max_get_rps = 2, s3_max_get_burst = 1,
                skip_access_check = 1)"
    $CLICKHOUSE_CLIENT -q "SYSTEM STOP MERGES $1"
    $CLICKHOUSE_CLIENT -q "
        INSERT INTO $1 SELECT number, number, number, number, number, number FROM numbers(24)
        SETTINGS max_block_size = 1, min_insert_block_size_rows = 1, min_insert_block_size_bytes = 0,
            max_insert_threads = 1, materialize_statistics_on_insert = 1, materialize_statistics_on_insert_max_table_size = 0,
            s3_check_objects_after_upload = 0"
    $CLICKHOUSE_CLIENT -q "SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = '$1' AND active"
}

function wait_for_refresh()
{
    for _ in {1..600}; do
        [[ $($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.background_schedule_pool
            WHERE database = currentDatabase() AND table = '$1' AND log_name = 'MergeTreeData::refreshStatistics'
                AND executing AND elapsed_ms > 1000") == 1 ]] && return
        sleep 0.1
    done
    echo "the statistics refresh of $1 is not running"
}

create_table t_alter
create_table t_drop

$CLICKHOUSE_CLIENT -q "ALTER TABLE t_alter MODIFY SETTING refresh_statistics_interval = 300"
wait_for_refresh t_alter
$CLICKHOUSE_CLIENT --query_id "${CLICKHOUSE_TEST_UNIQUE_NAME}_alter" -q "ALTER TABLE t_alter MODIFY SETTING refresh_statistics_interval = 299"
# The interrupted refresh published nothing, so the task armed by the ALTER refreshes again.
wait_for_refresh t_alter

$CLICKHOUSE_CLIENT -q "ALTER TABLE t_drop MODIFY SETTING refresh_statistics_interval = 300"
wait_for_refresh t_drop
$CLICKHOUSE_CLIENT --query_id "${CLICKHOUSE_TEST_UNIQUE_NAME}_drop" -q "DROP TABLE t_drop SYNC"
$CLICKHOUSE_CLIENT -q "DROP TABLE t_alter SYNC"

$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"
$CLICKHOUSE_CLIENT -q "
    SELECT substring(query_id, length('${CLICKHOUSE_TEST_UNIQUE_NAME}_') + 1), query_duration_ms < 20000
    FROM system.query_log
    WHERE event_date >= yesterday() AND current_database = currentDatabase() AND type = 'QueryFinish'
        AND startsWith(query_id, '${CLICKHOUSE_TEST_UNIQUE_NAME}_')
    ORDER BY 1"
