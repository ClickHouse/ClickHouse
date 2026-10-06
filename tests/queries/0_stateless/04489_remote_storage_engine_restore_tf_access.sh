#!/usr/bin/env bash
# Tags: shard

# Regression: a backup `RESTORE` of a `Remote` storage engine over a *table-function* target that
# resolves to a local shard must enforce the same access check that a direct `CREATE` does. A backup
# introduces a new table definition under the restoring user, so a user who can restore must not be
# able to smuggle in `Remote('127.0.0.1', merge(db, '^protected$'), 'default')` (with explicit columns)
# to reach a local target they cannot access directly.
#
# The restore still tolerates a genuinely absent target (see 04341): analysis is allowed to fail for
# reasons other than access control, because the table-function's underlying tables may legitimately be
# gone in the restore environment. Only an access-control failure (`ACCESS_DENIED`) is fatal, because
# that is the exact case a direct `CREATE` would reject and the only one that could leak a local target.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

db=${CLICKHOUSE_DATABASE}
user="user_04489_${CLICKHOUSE_DATABASE}"
backup="Disk('backups', '04489_${CLICKHOUSE_TEST_UNIQUE_NAME}')"
backup_absent="Disk('backups', '04489_absent_${CLICKHOUSE_TEST_UNIQUE_NAME}')"

# The admin creates the protected target and a `Remote` table whose local-shard table-function target
# (`merge(...)`) reads from it (the admin has access), then backs the `Remote` table up and drops it.
${CLICKHOUSE_CLIENT} <<EOF
DROP USER IF EXISTS $user;
CREATE TABLE $db.protected_target (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO $db.protected_target VALUES (42);
CREATE TABLE $db.t_remote (x UInt64) ENGINE = Remote('127.0.0.1', merge('$db', '^protected_target\$'), 'default');
EOF
${CLICKHOUSE_CLIENT} --query "BACKUP TABLE $db.t_remote TO $backup FORMAT Null"
${CLICKHOUSE_CLIENT} --query "DROP TABLE $db.t_remote SYNC"

# A restricted user that may restore tables into its own database but holds no rights on the protected
# local target the table-function target reads from.
${CLICKHOUSE_CLIENT} <<EOF
CREATE USER $user;
GRANT CREATE TABLE, SELECT, INSERT ON $db.* TO $user;
GRANT TABLE ENGINE ON Remote TO $user;
GRANT REMOTE ON *.* TO $user;
REVOKE SELECT, INSERT ON $db.protected_target FROM $user;
EOF

echo "-- restore without access to the local table-function target: rejected like CREATE"
${CLICKHOUSE_CLIENT} --user "$user" --query \
    "RESTORE TABLE $db.t_remote FROM $backup FORMAT Null" 2>&1 \
    | grep -c -m1 "ACCESS_DENIED\|Not enough privileges"
echo "-- the table was not created by the rejected restore"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.tables WHERE database = '$db' AND name = 't_remote'"

echo "-- restore with access to the local table-function target: allowed"
${CLICKHOUSE_CLIENT} --query "GRANT SELECT, INSERT ON $db.protected_target TO $user"
${CLICKHOUSE_CLIENT} --user "$user" --query "RESTORE TABLE $db.t_remote FROM $backup FORMAT Null"
${CLICKHOUSE_CLIENT} --user "$user" --query "SELECT x FROM $db.t_remote ORDER BY x"
${CLICKHOUSE_CLIENT} --query "DROP TABLE $db.t_remote SYNC"

# Absent target: a valid backup whose table-function target no longer exists must still be restorable,
# even by the restricted user, because a non-access analysis failure is tolerated (parity with 04341).
${CLICKHOUSE_CLIENT} <<EOF
CREATE TABLE $db.t_remote_absent (x UInt64) ENGINE = Remote('127.0.0.1', merge('$db', '^protected_target\$'), 'default');
EOF
${CLICKHOUSE_CLIENT} --query "BACKUP TABLE $db.t_remote_absent TO $backup_absent FORMAT Null"
${CLICKHOUSE_CLIENT} --query "DROP TABLE $db.t_remote_absent SYNC"
${CLICKHOUSE_CLIENT} --query "DROP TABLE $db.protected_target SYNC"

echo "-- restore of a Remote over an absent table-function target: allowed (tolerated non-access failure)"
# The tolerated absent-target fallback logs the swallowed analysis error at `WARNING`; silence it
# (`--send_logs_level=fatal` overrides the default `warning` via `--allow_repeated_settings`) so the
# expected server-log message relayed to the client does not trip the harness "having stderror" check.
${CLICKHOUSE_CLIENT} --send_logs_level=fatal --user "$user" --query "RESTORE TABLE $db.t_remote_absent FROM $backup_absent FORMAT Null"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.tables WHERE database = '$db' AND name = 't_remote_absent'"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS $db.t_remote_absent SYNC"
${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS $user"
