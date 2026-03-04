#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TZ=America/Hermosillo ${CLICKHOUSE_LOCAL} --query "select toDateTime64('1970-01-01 00:00:00.000', 3);"
TZ=America/Hermosillo ${CLICKHOUSE_LOCAL} --query "select toDateTime64('1970-01-01 00:59:59.999', 3);"
TZ=America/Hermosillo ${CLICKHOUSE_LOCAL} --query "select toDateTime64('1970-01-01 01:00:00.000', 3);"
TZ=America/Hermosillo ${CLICKHOUSE_LOCAL} --query "select toDateTime64('1969-12-31 23:59:59.999', 3);"
