#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CURL} -X POST -F 'query=select {p1:UInt8} + {p2:UInt8}' -F 'param_p1=3' -F 'param_p2=4' "${CLICKHOUSE_URL}"
