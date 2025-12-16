#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_FORMAT} --query "
SELECT (-'a')[4];
"
${CLICKHOUSE_FORMAT} --query "
SELECT (-('a')).1;
"
${CLICKHOUSE_FORMAT} --query "
SELECT (-'a').1;
"
${CLICKHOUSE_FORMAT} --query "
SELECT (-NULL)[1];
"
