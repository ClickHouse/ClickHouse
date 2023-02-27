#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

for tz in Asia/Tehran UTC Canada/Atlantic Europe/Berlin
do
    echo "$tz"
    TZ=$tz $CLICKHOUSE_LOCAL -mn < ${CUR_DIR}/02373_datetime64_monotonicity.queries
    echo ""
done
