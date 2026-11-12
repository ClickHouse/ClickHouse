#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: tar archives need libarchive, which is not in the fast test build.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

base="Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}_base.tar')"

$CLICKHOUSE_CLIENT -m -q "
    DROP TABLE IF EXISTS t;
    CREATE TABLE t (a UInt32) ENGINE = MergeTree ORDER BY a AS SELECT number FROM numbers(10);
    BACKUP TABLE t TO $base FORMAT Null;
"

# libarchive does not support passwords, so this must be a NOT_IMPLEMENTED error, not a LOGICAL_ERROR.
$CLICKHOUSE_CLIENT -q "RESTORE TABLE t AS t2 FROM $base SETTINGS password = 'x'" |& grep -m1 -o 'Cannot set password to tar archive. (NOT_IMPLEMENTED)'

# The base backup is opened with the same password.
$CLICKHOUSE_CLIENT -q "BACKUP TABLE t TO Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}_inc.zip') SETTINGS base_backup = $base, password = 'x', use_same_password_for_base_backup = 1" |& grep -m1 -o 'Cannot set password to tar archive. (NOT_IMPLEMENTED)'

$CLICKHOUSE_CLIENT -q "DROP TABLE t"
