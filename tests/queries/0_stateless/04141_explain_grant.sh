#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

# Test for EXPLAIN GRANT / EXPLAIN REVOKE: simulates a GRANT/REVOKE and returns
# the resulting `system.grants`-shaped rows for each affected grantee, without
# applying the change. Issue:
# https://github.com/ClickHouse/clickhouse-private/issues/36993
#
# User names are scoped by ${CLICKHOUSE_DATABASE}_$RANDOM so the test stays
# independent under flaky check / parallel runs (`local_directory` storage has
# a global user namespace). The output is normalised back to stable names for
# `.reference` comparison.
#
# Note: the inner query of `(EXPLAIN ...)` subquery wrapping is currently
# restricted to SELECT in the analyzer, so EXPLAIN GRANT runs at top level here
# and dumps all 9 columns of the `system.grants` schema.

u1="test_explain_grant_u1_${CLICKHOUSE_DATABASE}_$RANDOM"
u2="test_explain_grant_u2_${CLICKHOUSE_DATABASE}_$RANDOM"

normalize() {
    sed -e "s|${u1}|test_explain_grant_u1|g" -e "s|${u2}|test_explain_grant_u2|g"
}

${CLICKHOUSE_CLIENT} --multiquery <<EOF
DROP USER IF EXISTS ${u1}, ${u2};
CREATE USER ${u1}, ${u2};
EOF

# Single privilege on a single table.
echo 'simple grant'
${CLICKHOUSE_CLIENT} -q "EXPLAIN GRANT SELECT ON db.t TO ${u1}" | normalize

# Umbrella privilege: CREATE expands into CREATE DATABASE / TABLE / VIEW / DICTIONARY.
# Canonical example from the issue.
echo 'umbrella grant'
${CLICKHOUSE_CLIENT} -q "EXPLAIN GRANT CREATE ON foo.* TO ${u1}" | normalize

# WITH GRANT OPTION marks grant_option=1.
echo 'with grant option'
${CLICKHOUSE_CLIENT} -q "EXPLAIN GRANT SELECT ON *.* TO ${u1} WITH GRANT OPTION" | normalize

# Each grantee gets its own simulated post-state. Single-grantee queries
# avoid UUID-dependent ordering across grantees.
echo 'grantee u1'
${CLICKHOUSE_CLIENT} -q "EXPLAIN GRANT SELECT ON db.t TO ${u1}" | normalize
echo 'grantee u2'
${CLICKHOUSE_CLIENT} -q "EXPLAIN GRANT SELECT ON db.t TO ${u2}" | normalize

# Partial revoke after a wildcard grant: apply the wildcard for real, then
# EXPLAIN the REVOKE — output must include the surviving wildcard SELECT and
# a partial-revoke row for `system`.
${CLICKHOUSE_CLIENT} -q "GRANT SELECT ON *.* TO ${u1}"
echo 'partial revoke'
${CLICKHOUSE_CLIENT} -q "EXPLAIN REVOKE SELECT ON system.* FROM ${u1}" | normalize

# The GRANT was real, but the REVOKE was only EXPLAINed. The wildcard SELECT
# must still be the only row in `system.grants`, with no partial-revoke rows.
echo 'no side effect'
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.grants WHERE user_name = '${u1}' AND is_partial_revoke = 1"

# ON CLUSTER is rejected — explanation is local-only. The rejection must fire
# on the *original* AST, before `removeOnClusterClauseIfNeeded` would otherwise
# strip the clause when `ignore_on_cluster_for_replicated_access_entities_queries=1`
# with replicated access storage.
echo 'on cluster rejected'
${CLICKHOUSE_CLIENT} -q "EXPLAIN GRANT SELECT ON db.t TO ${u1} ON CLUSTER 'test_shard_localhost'" 2>&1 | grep -c 'BAD_ARGUMENTS' || true
${CLICKHOUSE_CLIENT} -q "EXPLAIN GRANT SELECT ON db.t TO ${u1} ON CLUSTER 'test_shard_localhost' SETTINGS ignore_on_cluster_for_replicated_access_entities_queries = 1" 2>&1 | grep -c 'BAD_ARGUMENTS' || true

${CLICKHOUSE_CLIENT} --multiquery <<EOF
DROP USER ${u1}, ${u2};
EOF
