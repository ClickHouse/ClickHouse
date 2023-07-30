#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --help | grep -q "\-\-host" && echo "OK" || echo "FAIL"
${CLICKHOUSE_CLIENT} --help | grep -q "\-\-port arg" && echo "OK" || echo "FAIL"

