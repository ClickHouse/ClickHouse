#!/usr/bin/env bash
# Tags: no-parallel, no-replicated-database
# Regression test for https://github.com/ClickHouse/ClickHouse/issues/98615
# Creating a table via ON CLUSTER with the same UUID as an existing database should give
# a proper error, not trigger an assertion failure in DatabaseCatalog::getTableImpl.
#
# The ON CLUSTER path causes DDLWorker::processTask to call tryGetTable (via getTableImpl)
# before executing the DDL. Without the fix, getTableImpl hits:
#   assert(!db_and_table.first && !db_and_table.second)
# because the UUID maps to a database entry {db_ptr, nullptr} rather than a missing entry.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -q "DROP DATABASE IF EXISTS d_uuid_collision_98615"
${CLICKHOUSE_CLIENT} -q "CREATE DATABASE d_uuid_collision_98615 UUID '10000000-0000-0000-0000-000000000001' ENGINE = Atomic"

# Use 'never_throw' to get the result set without raising an exception,
# then extract only the error code column to keep the reference output stable
# (the error message column contains a version string that changes across releases).
${CLICKHOUSE_CLIENT} -q "
    SET distributed_ddl_output_mode = 'never_throw';
    CREATE TABLE d_uuid_collision_98615.t0 UUID '10000000-0000-0000-0000-000000000001'
        ON CLUSTER test_shard_localhost
        (c0 Int32) ENGINE = MergeTree() ORDER BY tuple()
" | awk -F'\t' '{print $3}'

# Verify the server is still alive (no assertion failure occurred).
${CLICKHOUSE_CLIENT} -q "SELECT 1"

${CLICKHOUSE_CLIENT} -q "DROP DATABASE d_uuid_collision_98615"
