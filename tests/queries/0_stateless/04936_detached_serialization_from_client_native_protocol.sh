#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# We should have correct env vars from shell_config.sh to run this test
python3 "$CUR_DIR"/04936_detached_serialization_from_client_native_protocol.python

# The kinds a client may legitimately select still work, and the server is still alive.
$CLICKHOUSE_CLIENT -q "SELECT 'server alive', sum(number) FROM numbers(10)"
