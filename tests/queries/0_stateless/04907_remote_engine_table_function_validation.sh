#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Tags: no-parallel
# no-parallel: this test creates and drops a named collection, which is global server state.

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS remote_04907"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS src_04907"
${CLICKHOUSE_CLIENT} --query "DROP NAMED COLLECTION IF EXISTS nc_04907"

${CLICKHOUSE_CLIENT} --query "CREATE NAMED COLLECTION nc_04907 AS addresses_expr = '127.0.0.1', database = '', table = 'src_04907'"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE src_04907 (n UInt64) ENGINE = MergeTree ORDER BY n"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE remote_04907 ENGINE = Remote('127.0.0.1', remote(nc_04907, table = 'src_04907'))"

${CLICKHOUSE_CLIENT} --query "DROP NAMED COLLECTION nc_04907" 2>&1 | grep -F "NAMED_COLLECTION_IS_USED"
${CLICKHOUSE_CLIENT} --query "DETACH TABLE remote_04907"
${CLICKHOUSE_CLIENT} --query "ATTACH TABLE remote_04907"
${CLICKHOUSE_CLIENT} --query "DROP NAMED COLLECTION nc_04907" 2>&1 | grep -F "NAMED_COLLECTION_IS_USED"

${CLICKHOUSE_CLIENT} --query "DROP TABLE remote_04907"
${CLICKHOUSE_CLIENT} --query "DROP NAMED COLLECTION nc_04907"
${CLICKHOUSE_CLIENT} --query "DROP TABLE src_04907"

# A remote-only table with explicit columns may defer a missing backing object, but malformed table-function
# arguments must be rejected at definition time instead of failing only on the first read.
${CLICKHOUSE_CLIENT} --query "CREATE TABLE remote_invalid_04907 (n UInt64) ENGINE = Remote('127.0.0.1:1', dictionary())" 2>&1 | grep -F "Table function ('dictionary') requires 1 arguments"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE distributed_invalid_04907 (n UInt64) ENGINE = Distributed(test_cluster_multiple_nodes_all_unavailable, merge(1))" 2>&1 | grep -F "Argument 'table_name_regexp' must be a literal with type String, got UInt64"
