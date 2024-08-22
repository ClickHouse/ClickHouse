#!/usr/bin/env bash
# Tags: no-replicated-database, zookeeper

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS sample_table;"
${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS sample_table_2;"

${CLICKHOUSE_CLIENT} --query="CREATE TABLE sample_table (
    key UInt64
)
ENGINE ReplicatedMergeTree('/clickhouse/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/02221_system_zookeeper_unrestricted_like', '1')
ORDER BY tuple();
DROP TABLE IF EXISTS sample_table SYNC;"


${CLICKHOUSE_CLIENT} --query "CREATE TABLE sample_table_2 (
    key UInt64
)
ENGINE ReplicatedMergeTree('/clickhouse/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/02221_system_zookeeper_unrestricted_like_2', '1')
ORDER BY tuple();"

${CLICKHOUSE_CLIENT} --allow_unrestricted_reads_from_keeper=1 --query="SELECT name FROM (SELECT path, name FROM system.zookeeper WHERE path LIKE '/clickhouse%' ORDER BY name) WHERE path LIKE '%$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/02221_system_zookeeper_unrestricted_like%'"

${CLICKHOUSE_CLIENT} --query="SELECT '-------------------------'"

${CLICKHOUSE_CLIENT} --allow_unrestricted_reads_from_keeper=1 --query="SELECT name FROM (SELECT path, name FROM system.zookeeper WHERE path LIKE '/clickhouse/%' ORDER BY name) WHERE path LIKE '%$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/02221_system_zookeeper_unrestricted_like%'"

${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS sample_table;"
${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS sample_table_2;"
