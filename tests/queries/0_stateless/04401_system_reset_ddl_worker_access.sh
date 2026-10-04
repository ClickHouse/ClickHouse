#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# SYSTEM RESET DDL WORKER must require the SYSTEM RESET DDL WORKER privilege.
# Unique, database-scoped user names so the test is safe to run in parallel with itself.
user_unpriv="u_unpriv_${CLICKHOUSE_DATABASE}"
user_priv="u_priv_${CLICKHOUSE_DATABASE}"
user_cluster="u_cluster_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS ${user_unpriv}, ${user_priv}, ${user_cluster}"
${CLICKHOUSE_CLIENT} --query "CREATE USER ${user_unpriv}"
${CLICKHOUSE_CLIENT} --query "CREATE USER ${user_priv}"
${CLICKHOUSE_CLIENT} --query "CREATE USER ${user_cluster}"
${CLICKHOUSE_CLIENT} --query "GRANT SYSTEM RESET DDL WORKER ON *.* TO ${user_priv}"
${CLICKHOUSE_CLIENT} --query "GRANT CLUSTER ON *.* TO ${user_cluster}"

# Local path: unprivileged user is denied.
${CLICKHOUSE_CLIENT} --user="${user_unpriv}" --query "SYSTEM RESET DDL WORKER" 2>&1 \
    | grep -q "ACCESS_DENIED" && echo "unprivileged: denied" || echo "unprivileged: NOT denied"

# Granted user holds the privilege. Use CHECK GRANT (non-mutating) instead of executing
# SYSTEM RESET DDL WORKER: an actual reset mutates the shared stateless server's global DDL
# worker state and can postpone ON CLUSTER DDL run by other concurrent tests. CHECK GRANT
# returns 1 when the grant is held, with no side effect.
${CLICKHOUSE_CLIENT} --user="${user_priv}" --query "CHECK GRANT SYSTEM RESET DDL WORKER ON *.*" \
    | grep -q "^1$" && echo "granted: has privilege" || echo "granted: missing privilege"

# ON CLUSTER path: a user that has CLUSTER (enough to issue ON CLUSTER DDL) but lacks
# SYSTEM RESET DDL WORKER must still be denied before the query is enqueued. The denial
# must cite the missing SYSTEM RESET DDL WORKER grant, not CLUSTER, which proves the new
# required access from getRequiredAccessForDDLOnCluster is what blocks the user.
${CLICKHOUSE_CLIENT} --user="${user_cluster}" --query "SYSTEM RESET DDL WORKER ON CLUSTER test_shard_localhost" 2>&1 \
    | grep -q "necessary to have the grant SYSTEM RESET DDL WORKER" \
    && echo "on cluster, cluster-only: denied" || echo "on cluster, cluster-only: NOT denied"

${CLICKHOUSE_CLIENT} --query "DROP USER ${user_unpriv}, ${user_priv}, ${user_cluster}"
