#!/usr/bin/env bash
# Tags: no-fasttest, no-random-settings

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

pushd "${CLICKHOUSE_TMP}" > /dev/null || exit

echo "max_block_size: 31337" > clickhouse-client.yaml
${CLICKHOUSE_CLIENT} --query "SELECT getSetting('max_block_size')"
rm clickhouse-client.yaml

echo "max_block_size: 31338" > clickhouse-client.yml
${CLICKHOUSE_CLIENT} --query "SELECT getSetting('max_block_size')"
rm clickhouse-client.yml

echo "<clickhouse><max_block_size>31339</max_block_size></clickhouse>" > clickhouse-client.xml
${CLICKHOUSE_CLIENT} --query "SELECT getSetting('max_block_size')"
rm clickhouse-client.xml

popd > /dev/null || exit
