#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

env TZ=UTC $CLICKHOUSE_CLIENT --use_client_time_zone=1 --query="SELECT toDateTime(1000000000)"
