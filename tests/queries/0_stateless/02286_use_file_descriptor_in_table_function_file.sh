#!/usr/bin/env bash
# Tags: no-parallel

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

echo -e "1,2\n3,4" > 02286_data.csv

$CLICKHOUSE_LOCAL --query "SELECT * FROM file(0, CSV)" < 02286_data.csv
$CLICKHOUSE_LOCAL --query "SELECT * FROM file(stdin, CSV)" < 02286_data.csv
$CLICKHOUSE_LOCAL --query "SELECT * FROM file('-', CSV)" < 02286_data.csv
$CLICKHOUSE_LOCAL --query "SELECT * FROM file(5, CSV)" 5< 02286_data.csv

rm 02286_data.csv
