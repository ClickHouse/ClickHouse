#!/usr/bin/env bash
# Tags: no-parallel
# `SYSTEM DROP QUERY PLAN CACHE` is guarded on two independent entrypoints: the local one in
# `InterpreterSystemQuery::execute` and the `ON CLUSTER` one in `getRequiredAccessForDDLOnCluster`
# (covered by `04836_system_cache_on_cluster_access_types`). This test pins the local one, so that
# removing or miswiring its `checkAccess` cannot stay green on the strength of the cluster path.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

granted_user="granted_user_05059_$CLICKHOUSE_DATABASE"
granular_user="granular_user_05059_$CLICKHOUSE_DATABASE"
other_cache_user="other_cache_user_05059_$CLICKHOUSE_DATABASE"
no_grant_user="no_grant_user_05059_$CLICKHOUSE_DATABASE"

${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS $granted_user, $granular_user, $other_cache_user, $no_grant_user"

# The privilege group, which is the parent of the granular one.
${CLICKHOUSE_CLIENT} --query "CREATE USER $granted_user IDENTIFIED WITH no_password"
${CLICKHOUSE_CLIENT} --query "GRANT DROP CACHE ON *.* TO $granted_user"

# The granular privilege alone.
${CLICKHOUSE_CLIENT} --query "CREATE USER $granular_user IDENTIFIED WITH no_password"
${CLICKHOUSE_CLIENT} --query "GRANT SYSTEM DROP QUERY PLAN CACHE ON *.* TO $granular_user"

# A neighbouring cache privilege must not satisfy the check.
${CLICKHOUSE_CLIENT} --query "CREATE USER $other_cache_user IDENTIFIED WITH no_password"
${CLICKHOUSE_CLIENT} --query "GRANT SYSTEM DROP QUERY CACHE ON *.* TO $other_cache_user"

${CLICKHOUSE_CLIENT} --query "CREATE USER $no_grant_user IDENTIFIED WITH no_password"

# Positive: both spellings of the command are accepted, under the group and under the granular privilege.
${CLICKHOUSE_CLIENT} --user "$granted_user" --query "SYSTEM DROP QUERY PLAN CACHE" && echo "ok"
${CLICKHOUSE_CLIENT} --user "$granular_user" --query "SYSTEM DROP QUERY PLAN CACHE" && echo "ok"
${CLICKHOUSE_CLIENT} --user "$granular_user" --query "SYSTEM CLEAR QUERY PLAN CACHE" && echo "ok"

# Negative: no privilege at all, and a different cache privilege. The denial must also name the
# privilege that is actually required.
denied() {
    local out
    out=$(${CLICKHOUSE_CLIENT} --user "$1" --query "SYSTEM DROP QUERY PLAN CACHE" 2>&1)
    if ! grep -qF "ACCESS_DENIED" <<< "$out"; then
        echo "FAIL: expected ACCESS_DENIED: $out"
    elif ! grep -qF "SYSTEM DROP QUERY PLAN CACHE ON *.*" <<< "$out"; then
        echo "FAIL: the denial does not name the required privilege: $out"
    else
        echo "denied"
    fi
}

denied "$no_grant_user"
denied "$other_cache_user"

${CLICKHOUSE_CLIENT} --query "DROP USER $granted_user, $granular_user, $other_cache_user, $no_grant_user"
