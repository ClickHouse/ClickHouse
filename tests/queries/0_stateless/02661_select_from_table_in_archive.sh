#!/usr/bin/env bash
# Tags: no-parallel

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

echo -e "1,2\n3,4" > 02661_data.csv
zip archive1.zip 02661_data.csv > /dev/null
zip archive2.zip 02661_data.csv > /dev/null

$CLICKHOUSE_LOCAL --query "SELECT * FROM file('archive1.zip :: 02661_data.csv')"
$CLICKHOUSE_LOCAL --query "SELECT c1 FROM file('archive{1..2}.zip :: 02661_data.csv')"

rm 02661_data.csv
rm archive1.zip
rm archive2.zip
