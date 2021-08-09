#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh



for _ in {1..10}; do $CLICKHOUSE_CLIENT -q "select number from remote('127.0.0.{2,3}', numbers(20)) limit 8 settings max_block_size = 2, unknown_packet_in_send_data=4, sleep_in_send_data_ms=100, async_socket_for_remote=1 format Null" > /dev/null 2>&1 || true; done
