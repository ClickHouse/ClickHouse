#!/usr/bin/env bash
# Tags: zookeeper, no-replicated-database
# Tag no-replicated-database: distributed_ddl_output_mode is none

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The row policy targets this test's own database and the entity names embed it too.
# A policy on a shared database (the test used to create one on `default.*`) denies the
# `default.*` tables to every other user while it exists, breaking concurrent tests -
# e.g. the XML dictionaries sourced from `default.ints` / `default.decimals`.
POLICY="02911_rowpolicy_${CLICKHOUSE_DATABASE}"
TEST_USER="02911_user_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -q "DROP ROW POLICY IF EXISTS $POLICY ON ${CLICKHOUSE_DATABASE}.* ON CLUSTER test_shard_localhost"
$CLICKHOUSE_CLIENT -q "DROP USER IF EXISTS $TEST_USER ON CLUSTER test_shard_localhost"

$CLICKHOUSE_CLIENT -q "CREATE USER $TEST_USER ON CLUSTER test_shard_localhost"
$CLICKHOUSE_CLIENT -q "CREATE ROW POLICY $POLICY ON CLUSTER test_shard_localhost ON ${CLICKHOUSE_DATABASE}.* USING 1 TO $TEST_USER"

$CLICKHOUSE_CLIENT -q "DROP ROW POLICY $POLICY ON ${CLICKHOUSE_DATABASE}.* ON CLUSTER test_shard_localhost"
$CLICKHOUSE_CLIENT -q "DROP USER $TEST_USER ON CLUSTER test_shard_localhost"
