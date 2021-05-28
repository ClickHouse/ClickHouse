#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

export NO_SHELL_CONFIG=1

for _ in {1..4}; do
    "$CURDIR"/00097_long_storage_buffer_race_condition.sh  > /dev/null 2>&1 &
done

wait

$CLICKHOUSE_CLIENT -q "SELECT 'Still alive'";
