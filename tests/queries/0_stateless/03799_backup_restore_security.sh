#!/usr/bin/env bash

# Test for security fixes related to BACKUP and RESTORE operations:
# 1. RESTORE should be forbidden in readonly mode
# 2. The 'internal' setting should not be allowed for initial queries
# 3. Permission check should happen before backup destination is opened (e.g., S3 connection)
#
# All tests use fake S3 URLs with invalid credentials. Since all security checks happen
# before any connection attempt, we should always get ACCESS_DENIED errors, not S3 errors.
# We set backup_restore_s3_retry_attempts=0 to avoid retries on connection failure
# when running against a version without the fix.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

backup_name="${CLICKHOUSE_DATABASE}_03799_backup_security"
user_name="test_03799_user_${CLICKHOUSE_DATABASE}"

function cleanup()
{
    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS test_backup_security"
    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS test_restored"
    $CLICKHOUSE_CLIENT -q "DROP USER IF EXISTS $user_name"
}
trap cleanup EXIT

$CLICKHOUSE_CLIENT -q "
DROP TABLE IF EXISTS test_backup_security;
CREATE TABLE test_backup_security (id Int32) ENGINE=MergeTree() ORDER BY id;
INSERT INTO test_backup_security VALUES (1), (2), (3);
"

# Test 1: RESTORE should be forbidden in readonly mode
# The readonly check happens before any backup destination is accessed.
# We use CLICKHOUSE_CLIENT_BINARY directly to avoid test framework injecting settings that conflict with readonly mode.
$CLICKHOUSE_CLIENT --readonly=1 --backup_restore_s3_retry_attempts=0 -q "RESTORE TABLE test_backup_security AS test_restored FROM S3('http://localhost:11111/test/backups/${backup_name}', 'INVALID_ACCESS_KEY', 'INVALID_SECRET')" 2>&1 | grep -q "ACCESS_DENIED" && echo "Test 1 OK" || echo "Test 1 FAIL"
# Verify that the table was not created
$CLICKHOUSE_CLIENT -q "SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = 'test_restored'"

# Test 2: The 'internal' setting should not be allowed for initial BACKUP query
# This check happens before any connection attempt.
$CLICKHOUSE_CLIENT -m -q "
BACKUP TABLE test_backup_security TO S3('http://localhost:11111/test/backups/${backup_name}_internal', 'INVALID_ACCESS_KEY', 'INVALID_SECRET') SETTINGS internal=1, backup_restore_s3_retry_attempts=0; -- { serverError ACCESS_DENIED }
"

# Test 3: The 'internal' setting should not be allowed for initial RESTORE query
# This check happens before any connection attempt.
$CLICKHOUSE_CLIENT -m -q "
RESTORE TABLE test_backup_security AS test_restored FROM S3('http://localhost:11111/test/backups/${backup_name}', 'INVALID_ACCESS_KEY', 'INVALID_SECRET') SETTINGS internal=1, backup_restore_s3_retry_attempts=0; -- { serverError ACCESS_DENIED }
"

# Test 4: User without BACKUP permission should get ACCESS_DENIED (not S3_ERROR)
# This tests that permission check happens before opening backup destination.
# We use S3 with invalid credentials - if we get ACCESS_DENIED instead of S3_ERROR,
# it proves the permission check happens before attempting to connect to S3.
$CLICKHOUSE_CLIENT -q "DROP USER IF EXISTS $user_name"
$CLICKHOUSE_CLIENT -q "CREATE USER $user_name"
$CLICKHOUSE_CLIENT -q "GRANT SELECT ON ${CLICKHOUSE_DATABASE}.test_backup_security TO $user_name"
# User has SELECT but not BACKUP permission - should get ACCESS_DENIED, not S3_ERROR
$CLICKHOUSE_CLIENT --user=$user_name -m -q "
BACKUP TABLE test_backup_security TO S3('http://localhost:11111/test/backups/${CLICKHOUSE_DATABASE}/no_permission_backup', 'INVALID_ACCESS_KEY', 'INVALID_SECRET') SETTINGS backup_restore_s3_retry_attempts=0; -- { serverError ACCESS_DENIED }
"
$CLICKHOUSE_CLIENT -q "DROP USER IF EXISTS $user_name"
