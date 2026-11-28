#!/usr/bin/env bash
# Tags: zookeeper

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

db="${CLICKHOUSE_DATABASE}_table_name_length"
table_name=$(printf '%*s' 211 '' | tr ' ' a)
zk_path="/test/${CLICKHOUSE_TEST_ZOOKEEPER_PREFIX}/replicated_db"

cleanup()
{
    ${CLICKHOUSE_CLIENT} --ignore-error --query "DROP DATABASE IF EXISTS \`${db}\` SYNC" >/dev/null 2>&1
}

trap cleanup EXIT

${CLICKHOUSE_CLIENT} --query "DROP DATABASE IF EXISTS \`${db}\` SYNC" >/dev/null
${CLICKHOUSE_CLIENT} --query "CREATE DATABASE \`${db}\` ENGINE = Replicated('${zk_path}', 'shard1', 'replica1')" >/dev/null

query="CREATE TABLE \`${db}\`.\`${table_name}\` (col String) ENGINE = MergeTree ORDER BY tuple()"
if output=$(${CLICKHOUSE_CLIENT} --query "$query" 2>&1); then
    echo "long table name was accepted"
elif [[ "$output" == *"ARGUMENT_OUT_OF_BOUND"* ]]; then
    echo "ARGUMENT_OUT_OF_BOUND"
else
    printf '%s\n' "$output"
fi
