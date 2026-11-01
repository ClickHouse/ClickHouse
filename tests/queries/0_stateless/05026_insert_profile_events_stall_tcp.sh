#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "create table t_05026_insert_stall (x UInt8) engine = Null"

# Sends one Data block over the native protocol, then stalls: the server must keep streaming
# ProfileEvents while it waits for the next block, not only in reaction to received packets.
python3 "$CURDIR"/helpers/insert_profile_events_stall_tcp.python

$CLICKHOUSE_CLIENT -q "drop table t_05026_insert_stall"
