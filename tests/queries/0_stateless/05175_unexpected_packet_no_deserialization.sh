#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS tab"
$CLICKHOUSE_CLIENT -q "CREATE TABLE tab (x UInt64, CONSTRAINT c CHECK x < 10) ENGINE = Memory"

# We should have correct env vars from shell_config.sh to run this test
python3 "$CUR_DIR"/05175_unexpected_packet_no_deserialization.python

# The server must still be alive after rejecting the unexpected packets.
$CLICKHOUSE_CLIENT -q "SELECT 'server alive', count() FROM tab"

$CLICKHOUSE_CLIENT -q "DROP TABLE tab"
