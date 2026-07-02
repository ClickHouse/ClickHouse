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

# --- Access control during restore ---
# RESTORE creates WORKLOAD/RESOURCE entities via storeEntity(), bypassing the interpreters' own access
# checks, so the privileges are enforced in RestorerFromBackup::checkAccessForObjectsFoundInBackup. A user
# that can restore these system tables but lacks CREATE WORKLOAD / CREATE RESOURCE must be rejected.
user="user04409_${CLICKHOUSE_DATABASE}"
$CLICKHOUSE_CLIENT -q "DROP USER IF EXISTS ${user}"
$CLICKHOUSE_CLIENT -q "CREATE USER ${user} NOT IDENTIFIED"

# Drop the entities so that a successful RESTORE below actually re-creates them.
$CLICKHOUSE_CLIENT -m -q "
    DROP WORKLOAD development;
    DROP WORKLOAD production;
    DROP WORKLOAD all;
    DROP RESOURCE 04409_io;
    DROP RESOURCE 04409_query;
    DROP RESOURCE 04409_mem;
"

echo '--- restore without CREATE WORKLOAD/RESOURCE is denied (missing grants reported) ---'
$CLICKHOUSE_CLIENT --user "${user}" -q "RESTORE TABLE system.workloads, TABLE system.resources FROM ${backup_name}" 2>&1 \
    | grep -o -E "CREATE WORKLOAD|CREATE RESOURCE" | sort -u

echo '--- after granting CREATE WORKLOAD and CREATE RESOURCE, restore succeeds ---'
$CLICKHOUSE_CLIENT -q "GRANT CREATE WORKLOAD, CREATE RESOURCE ON *.* TO ${user}"
$CLICKHOUSE_CLIENT --user "${user}" -q "RESTORE TABLE system.workloads, TABLE system.resources FROM ${backup_name}" | cut -f2

$CLICKHOUSE_CLIENT -q "DROP USER IF EXISTS ${user}"

# --- create_workloads_and_resources gates how RESTORE treats already-existing entities ---
# WORKLOAD and RESOURCE entities are intertwined and restored together, so a single mode governs both.
# At this point all entities exist (restored just above).
echo '--- create_workloads_and_resources=create fails when entities already exist ---'
$CLICKHOUSE_CLIENT -q "RESTORE TABLE system.workloads, TABLE system.resources FROM ${backup_name} SETTINGS create_workloads_and_resources = 'create'" 2>&1 \
    | grep -o -E "already exists" | head -1

echo '--- create_workloads_and_resources=if-not-exists skips existing entities ---'
$CLICKHOUSE_CLIENT -q "RESTORE TABLE system.workloads, TABLE system.resources FROM ${backup_name} SETTINGS create_workloads_and_resources = 'if-not-exists'" | cut -f2

# Locally change a workload, then show 'replace' restores it back to the backed-up definition.
$CLICKHOUSE_CLIENT -q "CREATE OR REPLACE WORKLOAD development IN all SETTINGS priority = 1, weight = 5"
echo '--- development after local change (weight = 5) ---'
$CLICKHOUSE_CLIENT -q "SELECT create_query FROM system.workloads WHERE name = 'development'"
echo '--- create_workloads_and_resources=replace overwrites existing entities ---'
$CLICKHOUSE_CLIENT -q "RESTORE TABLE system.workloads, TABLE system.resources FROM ${backup_name} SETTINGS create_workloads_and_resources = 'replace'" | cut -f2
echo '--- development after replace-restore (weight back to 1) ---'
$CLICKHOUSE_CLIENT -q "SELECT create_query FROM system.workloads WHERE name = 'development'"

cleanup
