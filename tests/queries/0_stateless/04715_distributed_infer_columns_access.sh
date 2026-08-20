#!/usr/bin/env bash
# Tags: shard, no-replicated-database
# no-replicated-database: on a replicated / shared-catalog database the DDL runs with no user, so the
# in-storage access check asserted here is a no-op and the deny path silently allows.
# Blocked on https://github.com/ClickHouse/ClickHouse/issues/111561 - re-enable when fixed.

# Regression coverage for the access check applied when a `Distributed` table omits its structure:
#   1. Inferring the structure from a local-shard target requires `SHOW_COLUMNS` on that target, so a
#      user who cannot describe the target cannot learn its columns by creating a `Distributed` over it.
#   2. With `SHOW_COLUMNS` granted, the structure is still inferred (the check does not break inference).
#   3. An explicit column list infers nothing and stays allowed without any rights on the target.
#   4. The same check applies to a temporary table, which reaches the engine through its own path.
#   5. A table whose structure was inferred still detaches and re-attaches after the grant is
#      revoked: the inferred columns are persisted into its metadata, so the short `ATTACH` carries
#      them and never re-infers.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

db=${CLICKHOUSE_DATABASE}
user="user_04715_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} <<EOF
DROP USER IF EXISTS $user;
CREATE TABLE $db.protected_target (x UInt64, secret_column String) ENGINE = MergeTree ORDER BY x;
INSERT INTO $db.protected_target VALUES (42, 'secret');

CREATE USER $user;
-- The user may create and read tables in its own database, but holds no rights on the target the
-- engine points at, so the check below is exercised in isolation.
GRANT CREATE TABLE, SELECT, INSERT ON $db.* TO $user;
-- CREATE TEMPORARY TABLE is a global-level privilege and cannot be granted on a database.
GRANT CREATE TEMPORARY TABLE ON *.* TO $user;
GRANT TABLE ENGINE ON Distributed TO $user;
GRANT REMOTE ON *.* TO $user;
REVOKE SELECT, INSERT ON $db.protected_target FROM $user;
EOF

echo "-- the user cannot describe the target directly"
${CLICKHOUSE_CLIENT} --user "$user" --query "DESCRIBE $db.protected_target" 2>&1 \
    | grep -c -m1 "ACCESS_DENIED\|Not enough privileges"

echo "-- 1. omitted columns, no rights on the target: rejected"
${CLICKHOUSE_CLIENT} --user "$user" --query \
    "CREATE TABLE $db.d ENGINE = Distributed('test_shard_localhost', '$db', 'protected_target')" 2>&1 \
    | grep -c -m1 "ACCESS_DENIED\|Not enough privileges"

echo "-- 1. nothing was created, so nothing was disclosed"
${CLICKHOUSE_CLIENT} --query \
    "SELECT count() FROM system.tables WHERE database = '$db' AND name = 'd'"

echo "-- 2. omitted columns, with SHOW COLUMNS on the target: allowed and inferred"
${CLICKHOUSE_CLIENT} --query "GRANT SHOW COLUMNS ON $db.protected_target TO $user"
${CLICKHOUSE_CLIENT} --user "$user" --query \
    "CREATE TABLE $db.d ENGINE = Distributed('test_shard_localhost', '$db', 'protected_target')"
${CLICKHOUSE_CLIENT} --user "$user" --query \
    "SELECT name, type FROM system.columns WHERE database = '$db' AND table = 'd' ORDER BY position"

echo "-- 3. explicit columns, no rights on the target: still allowed"
${CLICKHOUSE_CLIENT} --query "REVOKE SHOW COLUMNS ON $db.protected_target FROM $user"
${CLICKHOUSE_CLIENT} --user "$user" --query \
    "CREATE TABLE $db.d_explicit (x UInt64) ENGINE = Distributed('test_shard_localhost', '$db', 'protected_target')"
${CLICKHOUSE_CLIENT} --query \
    "SELECT count() FROM system.tables WHERE database = '$db' AND name = 'd_explicit'"

echo "-- 4. temporary table, omitted columns, no rights on the target: rejected"
${CLICKHOUSE_CLIENT} --user "$user" --query \
    "CREATE TEMPORARY TABLE tmp_d ENGINE = Distributed('test_shard_localhost', '$db', 'protected_target')" 2>&1 \
    | grep -c -m1 "ACCESS_DENIED\|Not enough privileges"

echo "-- 5. a detached table with an inferred structure re-attaches without the grant"
${CLICKHOUSE_CLIENT} --query "GRANT DROP TABLE, UNDROP TABLE ON $db.* TO $user"
${CLICKHOUSE_CLIENT} --user "$user" <<EOF
DETACH TABLE $db.d;
ATTACH TABLE $db.d;
SELECT name, type FROM system.columns WHERE database = '$db' AND table = 'd' ORDER BY position;
EOF

${CLICKHOUSE_CLIENT} <<EOF
DROP TABLE $db.d;
DROP TABLE $db.d_explicit;
DROP TABLE $db.protected_target;
DROP USER $user;
EOF
