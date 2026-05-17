#!/usr/bin/env bash
# Tags: replica

# Regression test for the `BuzzHouse (amd_tsan)` finding STID 2436-3d64:
# "Logical error: No user in current context, it's a bug"
#
# Reading from `system.current_roles` or `system.enabled_roles` inside a
# CREATE TABLE ... AS SELECT executed by `DDLWorker` (DatabaseReplicated path)
# crashes the server because the DDL worker's query context has no user
# attached when `distributed_ddl_use_initial_user_and_roles` is disabled (the
# default). `Context::getRolesInfo` already handles that case by returning an
# empty set; `StorageSystemCurrentRoles::fillData` and
# `StorageSystemEnabledRoles::fillData` did not — they called
# `Context::getUser`, which throws `LOGICAL_ERROR` and aborts in
# debug/sanitizer builds.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB="${CLICKHOUSE_DATABASE}_repldb"
ZK_PREFIX="${CLICKHOUSE_TEST_ZOOKEEPER_PREFIX}/${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} --query "CREATE DATABASE ${DB} engine = Replicated('/clickhouse/databases/${ZK_PREFIX}', '{shard}', '{replica}')"

# Both queries used to abort the server on debug/sanitizer builds; expect
# success (empty result set — the DDL worker context has no user, hence no
# current/enabled roles).
${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode=none --query "
CREATE TABLE ${DB}.t_current_roles
(role_name String, with_admin_option UInt8, is_default UInt8)
ENGINE = MergeTree() ORDER BY role_name
AS SELECT * FROM system.current_roles
"

${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode=none --query "
CREATE TABLE ${DB}.t_enabled_roles
(role_name String, with_admin_option UInt8, is_current UInt8, is_default UInt8)
ENGINE = MergeTree() ORDER BY role_name
AS SELECT * FROM system.enabled_roles
"

${CLICKHOUSE_CLIENT} --query "SELECT count() FROM ${DB}.t_current_roles"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM ${DB}.t_enabled_roles"

# Sanity check: server is still alive and a user-level read of the same
# system tables still works (returns empty for the `default` user with no
# explicit roles).
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.current_roles"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.enabled_roles"

${CLICKHOUSE_CLIENT} --query "DROP DATABASE ${DB}"
