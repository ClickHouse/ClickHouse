#!/usr/bin/env bash
# Tags: no-fasttest, no-shared-merge-tree
# no-fasttest: needs ZooKeeper for the Replicated database engine.
# no-shared-merge-tree: SharedMergeTree-backed inner tables use a different drop path.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

db="rdb_${CLICKHOUSE_DATABASE}"

# --distributed_ddl_output_mode=none: suppress the per-replica DDL status rows so only the
# count() results reach stdout.
${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode=none --query "DROP DATABASE IF EXISTS ${db} SYNC"
${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode=none --query "CREATE DATABASE ${db} ENGINE = Replicated('/test/${CLICKHOUSE_TEST_ZOOKEEPER_PREFIX}/rdb', 's1', 'r1')"

${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode=none --allow_experimental_time_series_table 1 --query "CREATE TABLE ${db}.ts ENGINE = TimeSeries"

# Outer TimeSeries table + 3 inner tables (metrics, samples, tags).
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.tables WHERE database = '${db}'"

# Before the fix the background dropTableFinally task retried the inner-table drop forever,
# leaking the inner tables (and DROP ... SYNC hung).
${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode=none --query "DROP TABLE ${db}.ts SYNC"

# All tables (outer + inner) must be gone.
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.tables WHERE database = '${db}'"

${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode=none --query "DROP DATABASE ${db} SYNC"
