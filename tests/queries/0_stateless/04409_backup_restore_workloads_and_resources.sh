#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest
# - no-parallel: the `all` workload is a single global root shared by the whole server
# - no-fasttest: BACKUP/RESTORE is not available in fasttest builds

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

backup_name="Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}')"

function cleanup()
{
    $CLICKHOUSE_CLIENT -m -q "
        DROP WORKLOAD IF EXISTS development;
        DROP WORKLOAD IF EXISTS production;
        DROP WORKLOAD IF EXISTS all;
        DROP RESOURCE IF EXISTS 04409_io;
        DROP RESOURCE IF EXISTS 04409_query;
        DROP RESOURCE IF EXISTS 04409_mem;
    "
}

# Start from a clean slate in case a previous run left entities behind.
cleanup

$CLICKHOUSE_CLIENT -m -q "
    CREATE RESOURCE 04409_io (WRITE DISK 04409_disk, READ DISK 04409_disk);
    CREATE RESOURCE 04409_query (QUERY);
    CREATE RESOURCE 04409_mem (MEMORY RESERVATION);
    CREATE WORKLOAD all SETTINGS max_io_requests = 100 FOR 04409_io, max_concurrent_threads = 16;
    CREATE WORKLOAD production IN all SETTINGS priority = 1, weight = 9, max_memory = '1Gi';
    CREATE WORKLOAD development IN all SETTINGS priority = 1, weight = 1;
"

echo '--- workloads before backup ---'
$CLICKHOUSE_CLIENT -q "SELECT name, create_query FROM system.workloads ORDER BY name"
echo '--- resources before backup ---'
$CLICKHOUSE_CLIENT -q "SELECT name, create_query FROM system.resources ORDER BY name"

echo '--- backup ---'
$CLICKHOUSE_CLIENT -q "BACKUP TABLE system.workloads, TABLE system.resources TO ${backup_name}" | cut -f2

$CLICKHOUSE_CLIENT -m -q "
    DROP WORKLOAD development;
    DROP WORKLOAD production;
    DROP WORKLOAD all;
    DROP RESOURCE 04409_io;
    DROP RESOURCE 04409_query;
    DROP RESOURCE 04409_mem;
"

echo '--- counts after drop (expect 0 and 0) ---'
$CLICKHOUSE_CLIENT -q "SELECT count() FROM system.workloads"
$CLICKHOUSE_CLIENT -q "SELECT count() FROM system.resources"

echo '--- restore ---'
$CLICKHOUSE_CLIENT -q "RESTORE TABLE system.workloads, TABLE system.resources FROM ${backup_name}" | cut -f2

echo '--- workloads after restore (must match before) ---'
$CLICKHOUSE_CLIENT -q "SELECT name, create_query FROM system.workloads ORDER BY name"
echo '--- resources after restore (must match before) ---'
$CLICKHOUSE_CLIENT -q "SELECT name, create_query FROM system.resources ORDER BY name"

cleanup
