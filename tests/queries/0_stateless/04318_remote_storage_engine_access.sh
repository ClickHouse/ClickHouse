#!/usr/bin/env bash
# Tags: shard, no-replicated-database
# no-replicated-database: on a replicated / shared-catalog database the DDL runs with no user, so the
# in-storage access check asserted here is a no-op and the deny path silently allows.
# Blocked on https://github.com/ClickHouse/ClickHouse/issues/111561 - re-enable when fixed.

# Regression coverage for the security and lifecycle guarantees of the persistent `Remote` engine:
#   1. Creating `Remote('127.0.0.1', ...)` that resolves to a local shard requires the creator to
#      hold `SELECT` and `INSERT` on the local target (the engine credentials could otherwise be used
#      to route a query back to this server, bypassing the caller's access rights).
#   2. When the structure is omitted, it is inferred under the creating user's context, so a user who
#      cannot describe the local target cannot create the engine over it.
#   3. A `Remote(named_collection, ...)` table registers a dependency on the named collection, so
#      `DROP NAMED COLLECTION` is rejected while the table exists.
#   4. An `Alias` local target reports its own target's columns, so inferring the structure from one
#      requires the privilege on that target, not only on the alias.
#   5. A refreshable view re-creating such a table as its target is not exempt from check 1, even
#      though the definition it replays is the one this server stored.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

db=${CLICKHOUSE_DATABASE}
user="user_04318_${CLICKHOUSE_DATABASE}"
collection="collection_04318_${CLICKHOUSE_DATABASE}"
protected_db="${CLICKHOUSE_DATABASE}_protected_04318"

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
# The assertion below needs the table attached. ast_fuzzer_any_query = 0: a fuzzed DETACH would stop the
# drop below being refused, and a `__fuzz_N` clone inheriting the collection reference would leave
# metadata naming the collection dropped at the end.
${CLICKHOUSE_CLIENT} <<EOF
SET ast_fuzzer_any_query = 0;
DROP NAMED COLLECTION IF EXISTS $collection;
CREATE NAMED COLLECTION $collection AS host = '127.0.0.1', database = '$db', table = 'local_target', user = 'default';
CREATE TABLE $db.t_remote_nc (x UInt64) ENGINE = Remote($collection);
EOF
${CLICKHOUSE_CLIENT} --query "DROP NAMED COLLECTION $collection" 2>&1 | grep -c -m1 "NAMED_COLLECTION_IS_USED\|is used by"
${CLICKHOUSE_CLIENT} --query "SET ast_fuzzer_any_query = 0; DROP TABLE $db.t_remote_nc"
${CLICKHOUSE_CLIENT} --query "DROP NAMED COLLECTION $collection"

echo "-- 4. an Alias target: rejected while only the alias itself is readable"
${CLICKHOUSE_CLIENT} <<EOF
CREATE TABLE $db.alias_target ENGINE = Alias('$db', 'local_target');
GRANT TABLE ENGINE ON Distributed TO $user;
-- Leaves the database-level grant covering the alias, so the alias is readable and only its
-- target is not: the check under test is the one on the target.
REVOKE SELECT, INSERT ON $db.local_target FROM $user;
EOF
${CLICKHOUSE_CLIENT} --user "$user" --query \
    "CREATE TABLE $db.t_remote_alias ENGINE = Remote('127.0.0.1', $db, alias_target, 'default')" 2>&1 \
    | grep -c -m1 "ACCESS_DENIED\|Not enough privileges"
${CLICKHOUSE_CLIENT} --user "$user" --query \
    "CREATE TABLE $db.t_dist_alias ENGINE = Distributed(test_shard_localhost, $db, alias_target)" 2>&1 \
    | grep -c -m1 "ACCESS_DENIED\|Not enough privileges"
${CLICKHOUSE_CLIENT} --user "$user" --query \
    "DESCRIBE remote('127.0.0.1', $db, alias_target)" 2>&1 \
    | grep -c -m1 "ACCESS_DENIED\|Not enough privileges"

echo "-- 4. an Alias target: a column-scoped privilege on its target is not enough"
${CLICKHOUSE_CLIENT} --query "GRANT SELECT(x) ON $db.local_target TO $user"
${CLICKHOUSE_CLIENT} --user "$user" --query \
    "CREATE TABLE $db.t_dist_alias ENGINE = Distributed(test_shard_localhost, $db, alias_target)" 2>&1 \
    | grep -c -m1 "ACCESS_DENIED\|Not enough privileges"
${CLICKHOUSE_CLIENT} --query "REVOKE SELECT(x) ON $db.local_target FROM $user"

echo "-- 4. an Alias target: with the privilege on its target, the structure is inferred"
${CLICKHOUSE_CLIENT} --query "GRANT SHOW COLUMNS ON $db.local_target TO $user"
${CLICKHOUSE_CLIENT} --user "$user" --query \
    "CREATE TABLE $db.t_dist_alias ENGINE = Distributed(test_shard_localhost, $db, alias_target)"
${CLICKHOUSE_CLIENT} --user "$user" --query "SELECT name, type FROM system.columns WHERE database = '$db' AND table = 't_dist_alias'"
${CLICKHOUSE_CLIENT} --user "$user" --query "DESCRIBE remote('127.0.0.1', $db, alias_target)" | cut -f1,2
${CLICKHOUSE_CLIENT} --query "DROP TABLE $db.t_dist_alias"
${CLICKHOUSE_CLIENT} --query "DROP TABLE $db.alias_target"

echo "-- 5. a non-append refresh re-creates the target, and the local-shard check still runs"
# A refresh replays the target's own stored definition, so a name in its `SETTINGS` clause is not
# re-judged; the access check is separate and still applies, because the replay runs under the view's
# definer at an arbitrary later time and the engine credentials reach the local target directly when
# `prefer_localhost_replica = 0` routes the write over a connection.
#
# The target of the `Remote` engine must live outside $db: `prepareRefresh` pre-checks
# SELECT/INSERT/CREATE TABLE/DROP TABLE on the database of the table it re-creates, and a partial
# revoke inside $db would deny the refresh there instead, before the check under test is reached.
#
# ast_fuzzer_runs = 0 on every statement below: the stress profile fuzzes DDL, and this arm's state
# spans several client invocations, so a fuzzed detach or clone would decide the grant oracle instead.
${CLICKHOUSE_CLIENT} <<EOF
SET ast_fuzzer_runs = 0;
CREATE DATABASE $protected_db;
CREATE TABLE $protected_db.protected_target (x UInt64) ENGINE = MergeTree ORDER BY x;
GRANT SELECT, INSERT ON $protected_db.protected_target TO $user;
GRANT CREATE VIEW, DROP TABLE ON $db.* TO $user;
-- Arm 4 leaves SELECT and INSERT revoked on one table of this database, and creating a refreshable
-- view pre-checks SELECT/INSERT/CREATE TABLE/DROP TABLE on the whole database of its target, so the
-- partial revoke has to be undone before the view exists.
GRANT SELECT, INSERT ON $db.local_target TO $user;
CREATE TABLE $db.mv_target (x UInt64) ENGINE = Remote('127.0.0.1', $protected_db, protected_target, 'default');
CREATE TABLE $db.mv_src (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO $db.mv_src VALUES (1);
CREATE MATERIALIZED VIEW $db.mv REFRESH EVERY 10 YEAR TO $db.mv_target
    DEFINER = $user SQL SECURITY DEFINER
    EMPTY AS SELECT x FROM $db.mv_src
    SETTINGS prefer_localhost_replica = 0, distributed_foreground_insert = 1;
EOF
# While the definer still holds the grants the refresh succeeds, so the deny below is the check firing
# rather than the refresh being broken for some other reason.
${CLICKHOUSE_CLIENT} --query "SET ast_fuzzer_runs = 0; SYSTEM REFRESH VIEW $db.mv; SYSTEM WAIT VIEW $db.mv" > /dev/null \
    && echo "refresh runs while the definer is granted"
${CLICKHOUSE_CLIENT} --query "SET ast_fuzzer_runs = 0; REVOKE SELECT, INSERT ON $protected_db.protected_target FROM $user"
${CLICKHOUSE_CLIENT} --query "SET ast_fuzzer_runs = 0; SYSTEM REFRESH VIEW $db.mv; SYSTEM WAIT VIEW $db.mv" 2>&1 \
    | grep -c -m1 "ACCESS_DENIED\|Not enough privileges"
${CLICKHOUSE_CLIENT} --query "SET ast_fuzzer_runs = 0; DROP VIEW $db.mv"
${CLICKHOUSE_CLIENT} --query "SET ast_fuzzer_runs = 0; DROP DATABASE $protected_db"

${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS $user"
${CLICKHOUSE_CLIENT} --query "DROP TABLE $db.local_target"
