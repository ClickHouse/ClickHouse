#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

table="t_04499"
cleanup_user="cleanup_user_04499_$CLICKHOUSE_DATABASE"
vparts_user="vparts_user_04499_$CLICKHOUSE_DATABASE"
# Global (ON *.*) grants: the no-table ON CLUSTER form checks a single global grant on the initiator.
cleanup_global_user="cleanup_global_user_04499_$CLICKHOUSE_DATABASE"
vparts_global_user="vparts_global_user_04499_$CLICKHOUSE_DATABASE"
# Holds only SYSTEM PULLING REPLICATION LOG (the grant the ON CLUSTER path wrongly required before the fix).
wrong_user="wrong_user_04499_$CLICKHOUSE_DATABASE"
# Holds SYSTEM PULLING REPLICATION LOG globally (ON *.*): the old grant the no-table branch wrongly required.
wrong_global_user="wrong_global_user_04499_$CLICKHOUSE_DATABASE"

${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS $cleanup_user, $vparts_user, $cleanup_global_user, $vparts_global_user, $wrong_user, $wrong_global_user"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS $table"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE $table (a UInt64) ENGINE = MergeTree ORDER BY a"

${CLICKHOUSE_CLIENT} --query "CREATE USER $cleanup_user IDENTIFIED WITH no_password"
${CLICKHOUSE_CLIENT} --query "GRANT CLUSTER ON *.* TO $cleanup_user"
${CLICKHOUSE_CLIENT} --query "GRANT SYSTEM CLEANUP ON $CLICKHOUSE_DATABASE.$table TO $cleanup_user"

${CLICKHOUSE_CLIENT} --query "CREATE USER $vparts_user IDENTIFIED WITH no_password"
${CLICKHOUSE_CLIENT} --query "GRANT CLUSTER ON *.* TO $vparts_user"
${CLICKHOUSE_CLIENT} --query "GRANT SYSTEM VIRTUAL PARTS UPDATE ON $CLICKHOUSE_DATABASE.$table TO $vparts_user"

${CLICKHOUSE_CLIENT} --query "CREATE USER $cleanup_global_user IDENTIFIED WITH no_password"
${CLICKHOUSE_CLIENT} --query "GRANT CLUSTER ON *.* TO $cleanup_global_user"
${CLICKHOUSE_CLIENT} --query "GRANT SYSTEM CLEANUP ON *.* TO $cleanup_global_user"

${CLICKHOUSE_CLIENT} --query "CREATE USER $vparts_global_user IDENTIFIED WITH no_password"
${CLICKHOUSE_CLIENT} --query "GRANT CLUSTER ON *.* TO $vparts_global_user"
${CLICKHOUSE_CLIENT} --query "GRANT SYSTEM VIRTUAL PARTS UPDATE ON *.* TO $vparts_global_user"

${CLICKHOUSE_CLIENT} --query "CREATE USER $wrong_user IDENTIFIED WITH no_password"
${CLICKHOUSE_CLIENT} --query "GRANT CLUSTER ON *.* TO $wrong_user"
${CLICKHOUSE_CLIENT} --query "GRANT SYSTEM PULLING REPLICATION LOG ON $CLICKHOUSE_DATABASE.$table TO $wrong_user"

${CLICKHOUSE_CLIENT} --query "CREATE USER $wrong_global_user IDENTIFIED WITH no_password"
${CLICKHOUSE_CLIENT} --query "GRANT CLUSTER ON *.* TO $wrong_global_user"
${CLICKHOUSE_CLIENT} --query "GRANT SYSTEM PULLING REPLICATION LOG ON *.* TO $wrong_global_user"

cluster="test_shard_localhost"
run() { ${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode none "$@"; }

is_cloud=$(${CLICKHOUSE_CLIENT} --query "SELECT value FROM system.build_options WHERE name = 'CLICKHOUSE_CLOUD'")

# SYSTEM VIRTUAL PARTS UPDATE is a private feature, so only the access check is common to both
# builds. Assert the grant holder is not denied, and pin the open-source outcome (the command is
# not implemented there, so it stops at BAD_ARGUMENTS after the access check).
vparts_allowed() {
    local out
    out=$(run --user "$1" --query "$2" 2>&1)
    if grep -qF ACCESS_DENIED <<< "$out"; then
        echo "FAIL: access denied: $out"
    elif [ "$is_cloud" = 1 ] || grep -qF BAD_ARGUMENTS <<< "$out"; then
        echo "ok"
    else
        echo "FAIL: expected BAD_ARGUMENTS on the open-source build: $out"
    fi
}

# The ON CLUSTER path must check the dedicated grant, not SYSTEM PULLING REPLICATION LOG.

# Holder of SYSTEM CLEANUP is allowed (command runs on MergeTree).
run --user "$cleanup_user" --query "SYSTEM STOP CLEANUP ON CLUSTER $cluster $CLICKHOUSE_DATABASE.$table" >/dev/null || exit 1
echo "ok"
vparts_allowed "$vparts_user" "SYSTEM STOP VIRTUAL PARTS UPDATE ON CLUSTER $cluster $CLICKHOUSE_DATABASE.$table"

# Holder of only SYSTEM PULLING REPLICATION LOG is denied both commands.
run --user "$wrong_user" --query "SYSTEM STOP CLEANUP ON CLUSTER $cluster $CLICKHOUSE_DATABASE.$table -- { serverError ACCESS_DENIED }"
run --user "$wrong_user" --query "SYSTEM STOP VIRTUAL PARTS UPDATE ON CLUSTER $cluster $CLICKHOUSE_DATABASE.$table -- { serverError ACCESS_DENIED }"

# No-table ON CLUSTER form: the patch also switched this branch from the global SYSTEM PULLING
# REPLICATION LOG grant to the dedicated global SYSTEM CLEANUP / SYSTEM VIRTUAL PARTS UPDATE grant.
# The no-table check is a single global grant on the initiator, so these need ON *.* grants.

# Holder of global SYSTEM CLEANUP is allowed. START (not STOP) only wakes cleanup threads: idempotent
# and holds no lock, so it stays parallel-safe despite touching all tables on the host.
run --user "$cleanup_global_user" --query "SYSTEM START CLEANUP ON CLUSTER $cluster" >/dev/null || exit 1
echo "ok"
# START, not STOP: both spellings share one access branch, and START cannot leave updates
# globally stopped for other tests if the private build does execute the command.
vparts_allowed "$vparts_global_user" "SYSTEM START VIRTUAL PARTS UPDATE ON CLUSTER $cluster"

# Holder of only global SYSTEM PULLING REPLICATION LOG is now denied both no-table commands.
# This is the discriminating case: the no-table branch is a single global check, so the old code
# would have allowed this user via the global SYSTEM PULLING REPLICATION LOG grant; the fix denies it.
run --user "$wrong_global_user" --query "SYSTEM STOP CLEANUP ON CLUSTER $cluster -- { serverError ACCESS_DENIED }"
run --user "$wrong_global_user" --query "SYSTEM STOP VIRTUAL PARTS UPDATE ON CLUSTER $cluster -- { serverError ACCESS_DENIED }"

${CLICKHOUSE_CLIENT} --query "DROP USER $cleanup_user, $vparts_user, $cleanup_global_user, $vparts_global_user, $wrong_user, $wrong_global_user"
${CLICKHOUSE_CLIENT} --query "DROP TABLE $table"
