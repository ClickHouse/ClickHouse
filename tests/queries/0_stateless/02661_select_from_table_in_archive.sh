#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

echo -e "1,2\n3,4" >${CLICKHOUSE_TEST_UNIQUE_NAME}_data.csv
zip ${CLICKHOUSE_TEST_UNIQUE_NAME}_archive1.zip ${CLICKHOUSE_TEST_UNIQUE_NAME}_data.csv > /dev/null
zip ${CLICKHOUSE_TEST_UNIQUE_NAME}_archive2.zip  ${CLICKHOUSE_TEST_UNIQUE_NAME}_data.csv > /dev/null

$CLICKHOUSE_LOCAL --query "SELECT * FROM file('${CLICKHOUSE_TEST_UNIQUE_NAME}_archive1.zip :: ${CLICKHOUSE_TEST_UNIQUE_NAME}_data.csv')"
$CLICKHOUSE_LOCAL --query "SELECT c1 FROM file('${CLICKHOUSE_TEST_UNIQUE_NAME}_archive{1..2}.zip :: ${CLICKHOUSE_TEST_UNIQUE_NAME}_data.csv')"

rm ${CLICKHOUSE_TEST_UNIQUE_NAME}_data.csv
rm ${CLICKHOUSE_TEST_UNIQUE_NAME}_archive1.zip
rm ${CLICKHOUSE_TEST_UNIQUE_NAME}_archive2.zip
