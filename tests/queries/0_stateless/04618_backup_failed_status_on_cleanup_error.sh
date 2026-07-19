#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: backups to Disk('backups') are not configured in the fast test

# Test for issue #92649: when the cleanup after a failed backup threw a second
# exception, the terminal status was never written and the operation stayed
# `CREATING_BACKUP` in `system.backups` forever.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

backup_name="${CLICKHOUSE_TEST_UNIQUE_NAME}"

$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT backup_cleanup_error"

$CLICKHOUSE_CLIENT -q "BACKUP TABLE ${CLICKHOUSE_DATABASE}.table_does_not_exist TO Disk('backups', '${backup_name}')" 2>/dev/null || echo 'backup failed'

$CLICKHOUSE_CLIENT -q "SELECT status FROM system.backups WHERE name LIKE '%${backup_name}%'"

$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT backup_cleanup_error"
