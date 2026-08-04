#!/usr/bin/env bash
# Tags: no-fasttest, distributed
# no-fasttest: this test requires that nodes make authenticated connections
# between each other, but shared secret auth requires encodeSHA256, which is not
# available in the fast test build.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Verify that SET ROLE is respected when querying Distributed tables.
# Previously, RemoteQueryExecutor sent all granted roles to shards instead of
# just the currently active roles, causing permissive row policies from all
# roles to be OR'd together and effectively returning all rows.
#
# Three conditions are required to exercise the remote-shard code path where the
# bug lives; without them the query is served in-process and the bug is hidden:
#   1. A cluster with an inter-server <secret> (test_cluster_interserver_secret),
#      so the shard subquery runs as the initial_user and the pushed roles apply.
#   2. prefer_localhost_replica=0, so the query goes through RemoteQueryExecutor
#      instead of the in-process localhost shortcut (which keeps SET ROLE).
#   3. A user with DEFAULT ROLE NONE, so the shard's effective roles come from the
#      pushed roles rather than from the user's default roles (the shard unions
#      default_roles with the pushed external_roles).
#
# test_cluster_interserver_secret has two shards over loopback pointing at the
# same node, so a Distributed read returns each row twice; use DISTINCT.
#
# The distributed reads pin serialize_query_plan=0 so the test always exercises
# the RemoteQueryExecutor path this change fixes. With serialize_query_plan=1
# (the "distributed plan" test variant) the shard subquery applies no row policy
# at all -- a separate, pre-existing bypass of row policies through Distributed
# tables that is unrelated to role propagation and that adding a policy on the
# Distributed table does not fix.
# See https://github.com/ClickHouse/ClickHouse/issues/112891

USER="user_${CLICKHOUSE_TEST_UNIQUE_NAME}"
ROLE_A="role_a_${CLICKHOUSE_TEST_UNIQUE_NAME}"
ROLE_B="role_b_${CLICKHOUSE_TEST_UNIQUE_NAME}"
LOCAL_TABLE="local_${CLICKHOUSE_TEST_UNIQUE_NAME}"
DIST_TABLE="dist_${CLICKHOUSE_TEST_UNIQUE_NAME}"
POLICY_A="policy_a_${CLICKHOUSE_TEST_UNIQUE_NAME}"
POLICY_B="policy_b_${CLICKHOUSE_TEST_UNIQUE_NAME}"

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS ${DIST_TABLE}"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS ${LOCAL_TABLE}"
${CLICKHOUSE_CLIENT} -q "DROP ROW POLICY IF EXISTS ${POLICY_A} ON ${LOCAL_TABLE}"
${CLICKHOUSE_CLIENT} -q "DROP ROW POLICY IF EXISTS ${POLICY_B} ON ${LOCAL_TABLE}"
${CLICKHOUSE_CLIENT} -q "DROP ROLE IF EXISTS ${ROLE_A}"
${CLICKHOUSE_CLIENT} -q "DROP ROLE IF EXISTS ${ROLE_B}"
${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${USER}"

${CLICKHOUSE_CLIENT} -q "CREATE TABLE ${LOCAL_TABLE} (workspace_id UInt32, data String) ENGINE = MergeTree ORDER BY workspace_id"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE ${DIST_TABLE} AS ${LOCAL_TABLE} ENGINE = Distributed(test_cluster_interserver_secret, currentDatabase(), '${LOCAL_TABLE}')"

${CLICKHOUSE_CLIENT} -q "INSERT INTO ${LOCAL_TABLE} VALUES (1, 'ws1_row1'), (1, 'ws1_row2'), (2, 'ws2_row1'), (2, 'ws2_row2')"

${CLICKHOUSE_CLIENT} -q "CREATE ROLE ${ROLE_A}"
${CLICKHOUSE_CLIENT} -q "CREATE ROLE ${ROLE_B}"
# DEFAULT ROLE NONE: roles are only granted, activated per query via SET ROLE.
${CLICKHOUSE_CLIENT} -q "CREATE USER ${USER} DEFAULT ROLE NONE"
${CLICKHOUSE_CLIENT} -q "GRANT ${ROLE_A}, ${ROLE_B} TO ${USER}"
${CLICKHOUSE_CLIENT} -q "GRANT SELECT ON ${CLICKHOUSE_DATABASE}.${LOCAL_TABLE} TO ${ROLE_A}, ${ROLE_B}"
${CLICKHOUSE_CLIENT} -q "GRANT SELECT ON ${CLICKHOUSE_DATABASE}.${DIST_TABLE} TO ${ROLE_A}, ${ROLE_B}"
${CLICKHOUSE_CLIENT} -q "GRANT REMOTE ON *.* TO ${USER}"

${CLICKHOUSE_CLIENT} -q "CREATE ROW POLICY ${POLICY_A} ON ${LOCAL_TABLE} USING workspace_id = 1 AS PERMISSIVE TO ${ROLE_A}"
${CLICKHOUSE_CLIENT} -q "CREATE ROW POLICY ${POLICY_B} ON ${LOCAL_TABLE} USING workspace_id = 2 AS PERMISSIVE TO ${ROLE_B}"

echo "--- SET ROLE role_a on local table: expect workspace_id=1 only ---"
${CLICKHOUSE_CLIENT} --user "${USER}" -q "SET ROLE ${ROLE_A}; SELECT workspace_id, data FROM ${LOCAL_TABLE} ORDER BY workspace_id, data"

echo "--- SET ROLE role_b on local table: expect workspace_id=2 only ---"
${CLICKHOUSE_CLIENT} --user "${USER}" -q "SET ROLE ${ROLE_B}; SELECT workspace_id, data FROM ${LOCAL_TABLE} ORDER BY workspace_id, data"

echo "--- SET ROLE role_a on distributed table: expect workspace_id=1 only ---"
${CLICKHOUSE_CLIENT} --user "${USER}" -q "SET ROLE ${ROLE_A}; SELECT DISTINCT workspace_id, data FROM ${DIST_TABLE} ORDER BY workspace_id, data SETTINGS prefer_localhost_replica = 0, serialize_query_plan = 0"

echo "--- SET ROLE role_b on distributed table: expect workspace_id=2 only ---"
${CLICKHOUSE_CLIENT} --user "${USER}" -q "SET ROLE ${ROLE_B}; SELECT DISTINCT workspace_id, data FROM ${DIST_TABLE} ORDER BY workspace_id, data SETTINGS prefer_localhost_replica = 0, serialize_query_plan = 0"

echo "--- Both roles on distributed table: expect all rows ---"
${CLICKHOUSE_CLIENT} --user "${USER}" -q "SET ROLE ${ROLE_A}, ${ROLE_B}; SELECT DISTINCT workspace_id, data FROM ${DIST_TABLE} ORDER BY workspace_id, data SETTINGS prefer_localhost_replica = 0, serialize_query_plan = 0"

${CLICKHOUSE_CLIENT} -q "DROP TABLE ${DIST_TABLE}"
${CLICKHOUSE_CLIENT} -q "DROP TABLE ${LOCAL_TABLE}"
${CLICKHOUSE_CLIENT} -q "DROP ROW POLICY ${POLICY_A} ON ${LOCAL_TABLE}"
${CLICKHOUSE_CLIENT} -q "DROP ROW POLICY ${POLICY_B} ON ${LOCAL_TABLE}"
${CLICKHOUSE_CLIENT} -q "DROP ROLE ${ROLE_A}"
${CLICKHOUSE_CLIENT} -q "DROP ROLE ${ROLE_B}"
${CLICKHOUSE_CLIENT} -q "DROP USER ${USER}"
