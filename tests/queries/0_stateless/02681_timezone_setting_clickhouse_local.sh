#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TZ=UTC $CLICKHOUSE_LOCAL -mn < ${CUR_DIR}/02681_timezone_setting_clickhouse_local.queries
