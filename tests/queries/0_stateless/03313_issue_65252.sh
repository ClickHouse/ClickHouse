#!/usr/bin/env bash
CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh


echo "----CLICKHOUSE SHORTCUT CHECKS-----"
$CLICKHOUSE_BINARY --version
$CLICKHOUSE_BINARY --host sdfsadf --version
$CLICKHOUSE_BINARY -h sdfsadf --version
$CLICKHOUSE_BINARY --port 9000 --version
$CLICKHOUSE_BINARY --user qwr --version
$CLICKHOUSE_BINARY -u qwr --version
$CLICKHOUSE_BINARY --password secret --version

export CH_TMP="${CLICKHOUSE_BINARY%clickhouse}ch"
echo "----CH SHORTCUT CHECKS-----"
$CH_TMP --version
$CH_TMP --host sdfsadf --version
$CH_TMP -h sdfsadf --version
$CH_TMP --port 9000 --version
$CH_TMP --user qwr --version
$CH_TMP -u qwr --version
$CH_TMP --password secret --version