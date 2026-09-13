#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "
    SELECT kqlBin(
        toDateTime64('2290-08-01 12:34:56.1234567', 7, 'Asia/Shanghai'),
        toIntervalNanosecond(1000000000));
    SELECT kqlBinAt(
        toDateTime64('2290-08-01 12:34:56.1234567', 7, 'Asia/Shanghai'),
        toIntervalNanosecond(1000000000),
        toDateTime64('2290-08-01 00:00:00', 7, 'Asia/Shanghai'));
" --multiquery
