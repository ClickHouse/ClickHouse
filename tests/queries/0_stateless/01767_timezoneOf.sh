#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TZ=Asia/Tehran $CLICKHOUSE_LOCAL --query "SELECT timezone(), timezoneOf(now()), timeZone(), timeZoneOf(toTimezone(toNullable(now()), 'Africa/Accra')), timeZoneOf(toTimeZone(now64(3), 'Pacific/Pitcairn'))"
