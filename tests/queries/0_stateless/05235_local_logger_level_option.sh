#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# `--logger.level` was accepted by `clickhouse local` but overwritten with `trace`, so `--logger.console=1 --logger.level=fatal`
# still printed every trace message. `--log-level` and `--logger.level` must behave the same.

for opt in --log-level --logger.level; do
    echo "$opt=fatal"
    ${CLICKHOUSE_LOCAL} --logger.console=1 "$opt"=fatal --query "SELECT 1" 2>&1 | grep -c '<Trace>\|<Debug>\|<Information>'
    echo "$opt=trace"
    ${CLICKHOUSE_LOCAL} --logger.console=1 "$opt"=trace --query "SELECT 1" 2>&1 | grep -c '<Trace>' | sed 's/^[1-9][0-9]*$/many/'
done
