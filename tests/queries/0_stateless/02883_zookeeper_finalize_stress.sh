#!/usr/bin/env bash
# Tags: long

# Regression test for possible CANNOT_READ_ALL_DATA during client termination

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

yes /keeper/api_version | head -n1000 | xargs -P30 -i $CLICKHOUSE_KEEPER_CLIENT -q "get '{}'" > /dev/null
