#!/usr/bin/env bash
# Tags: shard

# Regression coverage for the access guarantees of a `Distributed` table over a table function
# (`Distributed(cluster, table_function())`) when the cluster has a local shard. A query against such
# a table can be routed back to this server, so the table-function target must be analyzed under the
# creating user's context at create time - mirroring the persistent `Remote` engine (04318):
#   1. When the structure is omitted, it is inferred under the creating user's context, so a user who
#      cannot read the local target cannot create the engine over `loop(...)` of it.
#   2. When the structure is explicit, the target is still validated as an access check, so a user who
#      cannot read the local target cannot persist `Distributed(..., loop(secret))` either.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

db=${CLICKHOUSE_DATABASE}
user="user_04620_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} <<EOF
DROP USER IF EXISTS $user;
CREATE TABLE $db.secret (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO $db.secret VALUES (42);

CREATE USER $user;
-- The user may create and read tables in its own database, but starts without any rights on the
-- local target the table function points at, so the local-shard analysis below is exercised in isolation.
GRANT CREATE TABLE, SELECT, INSERT ON $db.* TO $user;
GRANT TABLE ENGINE ON Distributed TO $user;
GRANT REMOTE ON *.* TO $user;
GRANT CREATE TEMPORARY TABLE ON *.* TO $user;
REVOKE SELECT, INSERT ON $db.secret FROM $user;
EOF

echo "-- 1. omitted columns over loop(secret), no access on the local target: rejected"
${CLICKHOUSE_CLIENT} --user "$user" --query \
    "CREATE TABLE $db.dist_infer ENGINE = Distributed(test_shard_localhost, loop($db, secret))" 2>&1 \
    | grep -c -m1 "ACCESS_DENIED\|Not enough privileges"

echo "-- 1. omitted columns over loop(secret), with access: inferred from the local target"
${CLICKHOUSE_CLIENT} --query "GRANT SELECT ON $db.secret TO $user"
${CLICKHOUSE_CLIENT} --user "$user" --query \
    "CREATE TABLE $db.dist_infer ENGINE = Distributed(test_shard_localhost, loop($db, secret))"
${CLICKHOUSE_CLIENT} --user "$user" --query "SELECT name, type FROM system.columns WHERE database = '$db' AND table = 'dist_infer'"
${CLICKHOUSE_CLIENT} --query "DROP TABLE $db.dist_infer"

echo "-- 2. explicit columns over loop(secret), no access on the local target: still rejected at create"
${CLICKHOUSE_CLIENT} --query "REVOKE SELECT ON $db.secret FROM $user"
${CLICKHOUSE_CLIENT} --user "$user" --query \
    "CREATE TABLE $db.dist_explicit (x UInt64) ENGINE = Distributed(test_shard_localhost, loop($db, secret))" 2>&1 \
    | grep -c -m1 "ACCESS_DENIED\|Not enough privileges"

echo "-- 2. explicit columns over loop(secret), with access: allowed"
${CLICKHOUSE_CLIENT} --query "GRANT SELECT ON $db.secret TO $user"
${CLICKHOUSE_CLIENT} --user "$user" --query \
    "CREATE TABLE $db.dist_explicit (x UInt64) ENGINE = Distributed(test_shard_localhost, loop($db, secret))"
${CLICKHOUSE_CLIENT} --user "$user" --query "EXISTS $db.dist_explicit"
${CLICKHOUSE_CLIENT} --query "DROP TABLE $db.dist_explicit"

${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS $user"
${CLICKHOUSE_CLIENT} --query "DROP TABLE $db.secret"
