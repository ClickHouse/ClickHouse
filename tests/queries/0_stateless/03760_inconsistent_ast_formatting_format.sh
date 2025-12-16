#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_FORMAT} --query "
SELECT (-'a')[4] FROM numbers(1) FORMAT Null;
"
${CLICKHOUSE_FORMAT} --query "
SELECT (-('a')).1 FROM numbers(1) FORMAT Null;
"
