#!/usr/bin/env bash
# Tags: no-replicated-database, zookeeper, no-shared-merge-tree
# no-shared-merge-tree: depend on specific paths created by replicated tables

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS sample_table"
${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS sample_table_2"

${CLICKHOUSE_CLIENT} -q"
CREATE TABLE sample_table (
    key UInt64
)
ENGINE ReplicatedMergeTree('/clickhouse/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/02221_system_zookeeper_unrestricted', '1')
ORDER BY tuple();
"

${CLICKHOUSE_CLIENT} -q"
CREATE TABLE sample_table_2 (
    key UInt64
)
ENGINE ReplicatedMergeTree('/clickhouse/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/02221_system_zookeeper_unrestricted_2', '1')
ORDER BY tuple();
"

${CLICKHOUSE_CLIENT} --allow_unrestricted_reads_from_keeper=1 --query "SELECT name FROM (SELECT path, name FROM system.zookeeper ORDER BY name) WHERE path LIKE '%$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/02221_system_zookeeper_unrestricted%'";

${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS sample_table"
${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS sample_table_2"
