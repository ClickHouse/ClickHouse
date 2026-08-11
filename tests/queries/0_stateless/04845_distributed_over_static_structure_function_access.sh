#!/usr/bin/env bash
# Tags: shard

# Regression coverage for the create-time access check of a `Distributed` (and `Remote`) table over a
# table function when the cluster has a local shard and the function has a static structure.
# A static-structure function such as `numbers` does not look at its arguments when asked for its
# header, so header inference alone would not evaluate a scalar-subquery argument like
# `numbers((SELECT count() FROM secret))` - it would only run later, on the local shard, under the
# cluster credentials (e.g. with `prefer_localhost_replica = 0`). The target must therefore be fully
# executed under the creator's context at create time, so that such arguments are access-checked.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

db=${CLICKHOUSE_DATABASE}
user="user_04845_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} <<EOF
DROP USER IF EXISTS $user;
CREATE TABLE $db.secret (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO $db.secret VALUES (42), (43), (44);

CREATE USER $user;
-- The user may create and read tables in its own database, but starts without any rights on the
-- table the scalar-subquery argument reads from, so the create-time execution of the target is
-- exercised in isolation.
GRANT CREATE TABLE, SELECT, INSERT ON $db.* TO $user;
GRANT TABLE ENGINE ON Distributed TO $user;
GRANT REMOTE ON *.* TO $user;
GRANT CREATE TEMPORARY TABLE ON *.* TO $user;
REVOKE SELECT, INSERT ON $db.secret FROM $user;
EOF

echo "-- 1. explicit columns over numbers((SELECT ... FROM secret)), no access on secret: rejected at create"
${CLICKHOUSE_CLIENT} --user "$user" --query \
    "CREATE TABLE $db.dist_static (n UInt64) ENGINE = Distributed(test_shard_localhost, numbers(assumeNotNull((SELECT count() FROM $db.secret))))" 2>&1 \
    | grep -c -m1 "ACCESS_DENIED\|Not enough privileges"

echo "-- 1. omitted columns over numbers((SELECT ... FROM secret)), no access on secret: rejected at create"
${CLICKHOUSE_CLIENT} --user "$user" --query \
    "CREATE TABLE $db.dist_static ENGINE = Distributed(test_shard_localhost, numbers(assumeNotNull((SELECT count() FROM $db.secret))))" 2>&1 \
    | grep -c -m1 "ACCESS_DENIED\|Not enough privileges"

echo "-- 1. explicit columns over numbers((SELECT ... FROM secret)), with access: allowed and readable"
${CLICKHOUSE_CLIENT} --query "GRANT SELECT ON $db.secret TO $user"
${CLICKHOUSE_CLIENT} --user "$user" --query \
    "CREATE TABLE $db.dist_static (n UInt64) ENGINE = Distributed(test_shard_localhost, numbers(assumeNotNull((SELECT count() FROM $db.secret))))"
${CLICKHOUSE_CLIENT} --user "$user" --query "SELECT count() FROM $db.dist_static"
${CLICKHOUSE_CLIENT} --query "DROP TABLE $db.dist_static"
${CLICKHOUSE_CLIENT} --query "REVOKE SELECT ON $db.secret FROM $user"

echo "-- 2. Remote engine analogue, explicit columns, no access on secret: rejected at create"
${CLICKHOUSE_CLIENT} --user "$user" --query \
    "CREATE TABLE $db.remote_static (n UInt64) ENGINE = Remote('127.0.0.1', numbers(assumeNotNull((SELECT count() FROM $db.secret))))" 2>&1 \
    | grep -c -m1 "ACCESS_DENIED\|Not enough privileges"

echo "-- 2. Remote engine analogue, with access: allowed and readable"
${CLICKHOUSE_CLIENT} --query "GRANT SELECT ON $db.secret TO $user"
${CLICKHOUSE_CLIENT} --user "$user" --query \
    "CREATE TABLE $db.remote_static (n UInt64) ENGINE = Remote('127.0.0.1', numbers(assumeNotNull((SELECT count() FROM $db.secret))))"
${CLICKHOUSE_CLIENT} --user "$user" --query "SELECT count() FROM $db.remote_static"
${CLICKHOUSE_CLIENT} --query "DROP TABLE $db.remote_static"

${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS $user"
${CLICKHOUSE_CLIENT} --query "DROP TABLE $db.secret"
