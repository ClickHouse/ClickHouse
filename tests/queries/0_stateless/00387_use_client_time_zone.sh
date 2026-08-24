#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# NOTE: session_timezone overrides use_client_time_zone, disable it randomization
env TZ=UTC ${CLICKHOUSE_CLIENT} --session_timezone '' --use_client_time_zone=1 --query="SELECT toDateTime(1000000000)"
