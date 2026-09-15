#!/usr/bin/env bash
# Tags: zookeeper, no-replicated-database
# Tag no-replicated-database: distributed_ddl_output_mode is none

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `SET DEFAULT ROLE ... TO CURRENT_USER ON CLUSTER` must apply to the user who issued the query, not to the
# user the DDL worker executes the distributed query as. `CURRENT_USER` is therefore resolved on the initiator,
# before the query text is queued into the DDL log.
# Unique, database-scoped names so the test is safe to run in parallel with itself.
user="u_${CLICKHOUSE_DATABASE}"
role="r_${CLICKHOUSE_DATABASE}"

# `none` keeps the per-host status rows of every `ON CLUSTER` statement out of the result, regardless of how
# the server this runs against is configured.
CLICKHOUSE_CLIENT_NO_DDL_OUTPUT="${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode none"

${CLICKHOUSE_CLIENT_NO_DDL_OUTPUT} --query "DROP USER IF EXISTS ${user} ON CLUSTER test_shard_localhost"
${CLICKHOUSE_CLIENT_NO_DDL_OUTPUT} --query "DROP ROLE IF EXISTS ${role} ON CLUSTER test_shard_localhost"
${CLICKHOUSE_CLIENT_NO_DDL_OUTPUT} --query "CREATE ROLE ${role} ON CLUSTER test_shard_localhost"
${CLICKHOUSE_CLIENT_NO_DDL_OUTPUT} --query "CREATE USER ${user} ON CLUSTER test_shard_localhost"
${CLICKHOUSE_CLIENT_NO_DDL_OUTPUT} --query "GRANT ON CLUSTER test_shard_localhost ${role} TO ${user}"
# Enough to issue an `ON CLUSTER` query; notably not `ALTER USER`, which changing one's own default roles
# does not require.
${CLICKHOUSE_CLIENT_NO_DDL_OUTPUT} --query "GRANT ON CLUSTER test_shard_localhost CLUSTER ON *.* TO ${user}"

${CLICKHOUSE_CLIENT_NO_DDL_OUTPUT} --user "${user}" --query "SET DEFAULT ROLE ${role} TO CURRENT_USER ON CLUSTER test_shard_localhost"

echo -n 'the issuing user got the default role: '
${CLICKHOUSE_CLIENT} --query "SELECT default_roles_list = ['${role}'] FROM system.users WHERE name = '${user}'"

echo -n 'no other user did: '
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.users WHERE has(default_roles_list, '${role}') AND name != '${user}'"

# The same query as a role name, for comparison: it must still work and must not need the tag resolved.
${CLICKHOUSE_CLIENT_NO_DDL_OUTPUT} --query "SET DEFAULT ROLE NONE TO ${user} ON CLUSTER test_shard_localhost"
echo -n 'NONE cleared it: '
${CLICKHOUSE_CLIENT} --query "SELECT default_roles_list = [] FROM system.users WHERE name = '${user}'"

${CLICKHOUSE_CLIENT_NO_DDL_OUTPUT} --query "DROP USER ${user} ON CLUSTER test_shard_localhost"
${CLICKHOUSE_CLIENT_NO_DDL_OUTPUT} --query "DROP ROLE ${role} ON CLUSTER test_shard_localhost"
