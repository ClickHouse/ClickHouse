#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: custom disks are not configured in fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Every location on the local filesystem that a disk defined in SQL names has to be inside
# `custom_local_disks_base_directory`. Two ways around that fence, both of them closed.

OUTSIDE="${USER_FILES_PATH}/05217_outside/"

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_05217"
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE t_05217 (x UInt64) ENGINE = MergeTree ORDER BY x
    SETTINGS disk = disk(name = '05217_ok_${CLICKHOUSE_DATABASE}', type = local, path = '${CLICKHOUSE_DISKS_FILES}/05217_ok_${CLICKHOUSE_DATABASE}/')"

# `ALTER TABLE ... MODIFY SETTING disk = disk(...)` resolves the definition once as if it came from
# stored metadata, which registers the disk, and only then checks whether the change is possible. The
# fence has to reject the definition before that first resolution registers anything.
$CLICKHOUSE_CLIENT -q "
    ALTER TABLE t_05217 MODIFY SETTING disk = disk(name = '05217_alter', type = local, path = '$OUTSIDE')" 2>&1 \
    | grep -m1 -c -F "must be inside"

# The disk must not be left behind by the rejected statement, ...
$CLICKHOUSE_CLIENT -q "SELECT count() FROM system.disks WHERE name = '05217_alter'"
# ... and it must not have created its directory either: the check runs before the disk is built, so
# `DiskLocal::setup` never gets to create it.
test -e "$OUTSIDE" && echo "the rejected disk created ${OUTSIDE}" || echo "no directory"

$CLICKHOUSE_CLIENT -q "DROP TABLE t_05217"

# The disk name is chosen by the query, so it cannot be what exempts a disk from the fence.
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE t_05217_backup (x UInt64) ENGINE = MergeTree ORDER BY x
    SETTINGS disk = disk(name = 'backup', type = local, path = '$OUTSIDE')" 2>&1 \
    | grep -m1 -c -F "must be inside"

$CLICKHOUSE_CLIENT -q "SELECT count() FROM system.disks WHERE name = 'backup'"
test -e "$OUTSIDE" && echo "the rejected disk created ${OUTSIDE}" || echo "no directory"
