#!/usr/bin/env bash
# Tags: shard

# Regression coverage for the security and lifecycle guarantees of the persistent `Remote` engine:
#   1. Creating `Remote('127.0.0.1', ...)` that resolves to a local shard requires the creator to
#      hold `SELECT` and `INSERT` on the local target (the engine credentials could otherwise be used
#      to route a query back to this server, bypassing the caller's access rights).
#   2. When the structure is omitted, it is inferred under the creating user's context, so a user who
#      cannot describe the local target cannot create the engine over it.
#   3. A `Remote(named_collection, ...)` table registers a dependency on the named collection, so
#      `DROP NAMED COLLECTION` is rejected while the table exists.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

db=${CLICKHOUSE_DATABASE}
user="user_04318_${CLICKHOUSE_DATABASE}"
collection="collection_04318_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} <<EOF
DROP USER IF EXISTS $user;
CREATE TABLE $db.local_target (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO $db.local_target VALUES (42);

CREATE USER $user;
-- The user may create and read tables in its own database, but starts without any rights on the
-- local target the engine points at, so the local-shard check below is exercised in isolation.
GRANT CREATE TABLE, SELECT, INSERT ON $db.* TO $user;
GRANT TABLE ENGINE ON Remote TO $user;
GRANT REMOTE ON *.* TO $user;
REVOKE SELECT, INSERT ON $db.local_target FROM $user;
EOF

echo "-- 1. explicit columns, no SELECT/INSERT on the local target: rejected"
${CLICKHOUSE_CLIENT} --user "$user" --query \
    "CREATE TABLE $db.t_remote (x UInt64) ENGINE = Remote('127.0.0.1', $db, local_target, 'default')" 2>&1 \
    | grep -c -m1 "ACCESS_DENIED\|Not enough privileges"

echo "-- 1. with SELECT but without INSERT: still rejected"
${CLICKHOUSE_CLIENT} --query "GRANT SELECT ON $db.local_target TO $user"
${CLICKHOUSE_CLIENT} --user "$user" --query \
    "CREATE TABLE $db.t_remote (x UInt64) ENGINE = Remote('127.0.0.1', $db, local_target, 'default')" 2>&1 \
    | grep -c -m1 "ACCESS_DENIED\|Not enough privileges"

echo "-- 1. with both SELECT and INSERT: allowed"
${CLICKHOUSE_CLIENT} --query "GRANT INSERT ON $db.local_target TO $user"
${CLICKHOUSE_CLIENT} --user "$user" --query \
    "CREATE TABLE $db.t_remote (x UInt64) ENGINE = Remote('127.0.0.1', $db, local_target, 'default')"
${CLICKHOUSE_CLIENT} --user "$user" --query "SELECT x FROM $db.t_remote ORDER BY x"
${CLICKHOUSE_CLIENT} --query "DROP TABLE $db.t_remote"

echo "-- 2. omitted columns: inferred under the user's context (rejected without access)"
${CLICKHOUSE_CLIENT} --query "REVOKE SELECT, INSERT ON $db.local_target FROM $user"
${CLICKHOUSE_CLIENT} --user "$user" --query \
    "CREATE TABLE $db.t_remote_infer ENGINE = Remote('127.0.0.1', $db, local_target, 'default')" 2>&1 \
    | grep -c -m1 "ACCESS_DENIED\|Not enough privileges"

echo "-- 2. omitted columns: with access, the structure is inferred from the local target"
${CLICKHOUSE_CLIENT} --query "GRANT SELECT, INSERT ON $db.local_target TO $user"
${CLICKHOUSE_CLIENT} --user "$user" --query \
    "CREATE TABLE $db.t_remote_infer ENGINE = Remote('127.0.0.1', $db, local_target, 'default')"
${CLICKHOUSE_CLIENT} --user "$user" --query "SELECT name, type FROM system.columns WHERE database = '$db' AND table = 't_remote_infer'"
${CLICKHOUSE_CLIENT} --user "$user" --query "SELECT x FROM $db.t_remote_infer ORDER BY x"
${CLICKHOUSE_CLIENT} --query "DROP TABLE $db.t_remote_infer"

echo "-- 3. a Remote table built from a named collection blocks DROP NAMED COLLECTION"
${CLICKHOUSE_CLIENT} <<EOF
DROP NAMED COLLECTION IF EXISTS $collection;
CREATE NAMED COLLECTION $collection AS host = '127.0.0.1', database = '$db', table = 'local_target', user = 'default';
CREATE TABLE $db.t_remote_nc (x UInt64) ENGINE = Remote($collection);
EOF
${CLICKHOUSE_CLIENT} --query "DROP NAMED COLLECTION $collection" 2>&1 | grep -c -m1 "NAMED_COLLECTION_IS_USED\|is used by"
${CLICKHOUSE_CLIENT} --query "DROP TABLE $db.t_remote_nc"
${CLICKHOUSE_CLIENT} --query "DROP NAMED COLLECTION $collection"

${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS $user"
${CLICKHOUSE_CLIENT} --query "DROP TABLE $db.local_target"
