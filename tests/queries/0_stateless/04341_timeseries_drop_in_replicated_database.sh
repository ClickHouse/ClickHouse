#!/usr/bin/env bash
# Tags: no-fasttest, no-shared-merge-tree
# no-fasttest: needs ZooKeeper for the Replicated database engine.
# no-shared-merge-tree: SharedMergeTree-backed inner tables use a different drop path.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

db="rdb_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} --query "DROP DATABASE IF EXISTS ${db} SYNC"
${CLICKHOUSE_CLIENT} --query "CREATE DATABASE ${db} ENGINE = Replicated('/test/${CLICKHOUSE_TEST_ZOOKEEPER_PREFIX}/rdb', 's1', 'r1')"

${CLICKHOUSE_CLIENT} --allow_experimental_time_series_table 1 --query "CREATE TABLE ${db}.ts ENGINE = TimeSeries"

# Outer TimeSeries table + 3 inner tables (metrics, samples, tags).
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.tables WHERE database = '${db}'"

# Before the fix this DROP succeeds for the outer table but the background dropTableFinally
# task retries the inner-table drop forever, so the inner tables are never removed.
${CLICKHOUSE_CLIENT} --query "DROP TABLE ${db}.ts SYNC"

# All tables (outer + inner) must be gone. Without the fix the 3 inner tables leak.
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.tables WHERE database = '${db}'"

${CLICKHOUSE_CLIENT} --query "DROP DATABASE ${db} SYNC"
