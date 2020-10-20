#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --param_p1='ffffffff-ffff-ffff-ffff-ffffffffffff' --query "SELECT {p1:UUID}"
