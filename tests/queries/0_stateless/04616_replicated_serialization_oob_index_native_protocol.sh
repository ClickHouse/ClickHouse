#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS tab"
$CLICKHOUSE_CLIENT -q "CREATE TABLE tab (x UInt64) ENGINE = Memory"

# We should have correct env vars from shell_config.sh to run this test
python3 "$CUR_DIR"/04616_replicated_serialization_oob_index_native_protocol.python

# The server must still be alive after rejecting the malformed block.
$CLICKHOUSE_CLIENT -q "SELECT 'server alive', count() FROM tab"

$CLICKHOUSE_CLIENT -q "DROP TABLE tab"
