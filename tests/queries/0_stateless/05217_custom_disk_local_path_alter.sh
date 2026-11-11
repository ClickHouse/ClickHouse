#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# Tag no-fasttest: custom disks are not configured in fasttest
# Tag no-replicated-database: creates a user and a settings profile

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Every location on the local filesystem that a disk defined in SQL names has to be inside
# `custom_local_disks_base_directory`. `CREATE TABLE` checks that; `ALTER TABLE ... MODIFY SETTING disk`
# has to check it the same way.
# In this release the check runs after the disk is created, and does not cover object storage disks over
# the local filesystem, so only the rejection of the statement is checked here.

OUTSIDE="${USER_FILES_PATH}/05217_outside_${CLICKHOUSE_DATABASE}"
INSIDE="${CLICKHOUSE_DISKS_FILES}/05217_${CLICKHOUSE_DATABASE}"

# Prints 1 if the statement was rejected by the fence.
function check_rejected()
{
    local query=$1
    $CLICKHOUSE_CLIENT -q "$query" 2>&1 | grep -m1 -c -F "must be inside"
}

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_05217"
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE t_05217 (x UInt64) ENGINE = MergeTree ORDER BY x
    SETTINGS disk = disk(name = '05217_ok_${CLICKHOUSE_DATABASE}', type = local, path = '${INSIDE}_ok/')"

# `ALTER TABLE ... MODIFY SETTING disk = disk(...)` used to resolve the definition as if it came from
# stored metadata, which registered the disk unchecked, before the check of the statement ran.
echo "local"
check_rejected "
    ALTER TABLE t_05217 MODIFY SETTING disk = disk(name = '05217_local_${CLICKHOUSE_DATABASE}', type = local, path = '${OUTSIDE}_local/')"

$CLICKHOUSE_CLIENT -q "DROP TABLE t_05217"

# A table created without a SETTINGS clause goes through the same check.
echo "no settings clause"
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_05217_plain (x UInt64) ENGINE = MergeTree ORDER BY x"
check_rejected "
    ALTER TABLE t_05217_plain MODIFY SETTING disk = disk(name = '05217_plain_${CLICKHOUSE_DATABASE}', type = local, path = '${OUTSIDE}_plain/')"
$CLICKHOUSE_CLIENT -q "DROP TABLE t_05217_plain"

# The settings constraints compare the `disk` the table has after the `ALTER`, not one left unresolved:
# a constant `merge_tree_disk` rejects a change of the disk and nothing else.
echo "constraints"
user="u_05217_${CLICKHOUSE_DATABASE}"
profile="p_05217_${CLICKHOUSE_DATABASE}"
$CLICKHOUSE_CLIENT -q "
    DROP USER IF EXISTS ${user};
    DROP SETTINGS PROFILE IF EXISTS ${profile};
    CREATE USER ${user} IDENTIFIED WITH no_password;
    GRANT ALTER ON ${CLICKHOUSE_DATABASE}.* TO ${user};
    CREATE SETTINGS PROFILE ${profile} SETTINGS merge_tree_disk CONST TO ${user};
    CREATE TABLE t_05217_default (x UInt64) ENGINE = MergeTree ORDER BY x;
    CREATE TABLE t_05217_disk (x UInt64) ENGINE = MergeTree ORDER BY x SETTINGS disk = 'local_disk';"

$CLICKHOUSE_CLIENT --user "${user}" -q "ALTER TABLE t_05217_default MODIFY SETTING disk = 'local_disk'" 2>&1 \
    | grep -m1 -c -F "Setting disk should not be changed"
$CLICKHOUSE_CLIENT --user "${user}" -q "ALTER TABLE t_05217_disk MODIFY SETTING min_bytes_for_wide_part = 0" && echo "unrelated change allowed"

$CLICKHOUSE_CLIENT -q "
    DROP TABLE t_05217_default;
    DROP TABLE t_05217_disk;
    DROP SETTINGS PROFILE ${profile};
    DROP USER ${user};"

# In this release a rejected disk still creates its directory, so remove what the statements above left.
rm -rf "${OUTSIDE}"_*
