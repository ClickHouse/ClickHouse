#!/usr/bin/env bash
# Tags: shard

# Regression: a backup `RESTORE` of a `Remote` storage engine that resolves to a local shard must
# enforce the same local-target `SELECT`/`INSERT` access check that a direct `CREATE` does. A backup
# introduces a new table definition under the restoring user, so a user who can restore must not be
# able to smuggle in `Remote('127.0.0.1', protected_db, protected_table, 'default')` to reach a local
# target they cannot access directly. (Server startup loads already-validated metadata and still
# skips the check; previously `is_restore_from_backup` was folded into that same skip, which let a
# restore bypass the check.)

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

db=${CLICKHOUSE_DATABASE}
user="user_04401_${CLICKHOUSE_DATABASE}"
backup="Disk('backups', '04401_${CLICKHOUSE_TEST_UNIQUE_NAME}')"

# The admin creates the protected target and a `Remote` table over it (it has access), then backs the
# `Remote` table up and drops it.
${CLICKHOUSE_CLIENT} <<EOF
DROP USER IF EXISTS $user;
CREATE TABLE $db.protected_target (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO $db.protected_target VALUES (42);
CREATE TABLE $db.t_remote (x UInt64) ENGINE = Remote('127.0.0.1', $db, protected_target, 'default');
EOF
${CLICKHOUSE_CLIENT} --query "BACKUP TABLE $db.t_remote TO $backup FORMAT Null"
${CLICKHOUSE_CLIENT} --query "DROP TABLE $db.t_remote SYNC"

# A restricted user that may restore tables into its own database but holds no rights on the protected
# local target the engine points at.
${CLICKHOUSE_CLIENT} <<EOF
CREATE USER $user;
GRANT CREATE TABLE, SELECT, INSERT ON $db.* TO $user;
GRANT TABLE ENGINE ON Remote TO $user;
GRANT REMOTE ON *.* TO $user;
REVOKE SELECT, INSERT ON $db.protected_target FROM $user;
EOF

echo "-- restore without SELECT/INSERT on the protected local target: rejected like CREATE"
${CLICKHOUSE_CLIENT} --user "$user" --query \
    "RESTORE TABLE $db.t_remote FROM $backup FORMAT Null" 2>&1 \
    | grep -c -m1 "ACCESS_DENIED\|Not enough privileges"
echo "-- the table was not created by the rejected restore"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.tables WHERE database = '$db' AND name = 't_remote'"

echo "-- restore with both SELECT and INSERT on the protected local target: allowed"
${CLICKHOUSE_CLIENT} --query "GRANT SELECT, INSERT ON $db.protected_target TO $user"
${CLICKHOUSE_CLIENT} --user "$user" --query "RESTORE TABLE $db.t_remote FROM $backup FORMAT Null"
${CLICKHOUSE_CLIENT} --user "$user" --query "SELECT x FROM $db.t_remote ORDER BY x"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS $db.t_remote SYNC"
${CLICKHOUSE_CLIENT} --query "DROP TABLE $db.protected_target"
${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS $user"
