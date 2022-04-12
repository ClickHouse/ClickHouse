#!/usr/bin/env bash
# Tags: long, zookeeper

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh


${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS test_01753";
${CLICKHOUSE_CLIENT} --query="CREATE TABLE test_01753 (n Int8) ENGINE=ReplicatedMergeTree('/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/test_01753/test', '1') ORDER BY n"

${CLICKHOUSE_CLIENT} --query="SELECT name FROM system.zookeeper WHERE path = {path:String}" --param_path "$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/test_01753"


${CLICKHOUSE_CLIENT} --query="DROP TABLE test_01753 SYNC";
