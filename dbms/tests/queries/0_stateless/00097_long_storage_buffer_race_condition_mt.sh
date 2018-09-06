#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

export NO_SHELL_CONFIG=1

$CURDIR/00097_long_storage_buffer_race_condition.sh &
$CURDIR/00097_long_storage_buffer_race_condition.sh &
$CURDIR/00097_long_storage_buffer_race_condition.sh &
$CURDIR/00097_long_storage_buffer_race_condition.sh &

wait

$CLICKHOUSE_CLIENT -q "SELECT 'Still alive'";
