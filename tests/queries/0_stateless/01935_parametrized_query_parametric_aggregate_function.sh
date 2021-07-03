#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CURL} -sS -XPOST "${CLICKHOUSE_URL}&param_lim=2" --data-binary 'select length(topKArray({lim:UInt32})([1,1,2,3,4,5,6,7,7,7]))'
