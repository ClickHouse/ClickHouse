#!/usr/bin/env bash
# Tags: no-fasttest

# Test the `send_header_and_column_defaults_for_insert` setting.
# When it is disabled, the server does not send the header block and column defaults
# to the client before receiving the INSERT data over the native protocol.
# `clickhouse-client` with the setting disabled sends INSERT queries with inline data
# as-is (the server parses the data), and rejects INSERT queries with external data,
# because it cannot parse them without the structure from the server.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS test_04512"
$CLICKHOUSE_CLIENT --query "CREATE TABLE test_04512 (x UInt64, y String DEFAULT 'y_default') ENGINE = MergeTree ORDER BY x"

# Inline data is sent as-is and parsed by the server; column defaults are applied by the server.
$CLICKHOUSE_CLIENT --send_header_and_column_defaults_for_insert 0 --query "INSERT INTO test_04512 VALUES (1, 'hello')"
$CLICKHOUSE_CLIENT --send_header_and_column_defaults_for_insert 0 --query "INSERT INTO test_04512 (x) VALUES (2)"
$CLICKHOUSE_CLIENT --send_header_and_column_defaults_for_insert 0 --query "INSERT INTO test_04512 FORMAT JSONEachRow {\"x\": 3}"

# INSERT with external data (from stdin) is rejected, because the client needs
# the table structure from the server to parse it.
$CLICKHOUSE_CLIENT --send_header_and_column_defaults_for_insert 0 --query "INSERT INTO test_04512 FORMAT TSV" <<<"4	stdin" 2>&1 | grep -F -c "requires receiving the table structure from the server"

# Raw native protocol: with the setting disabled the server must send neither
# `TableColumns` nor the header block, and the INSERT must still succeed.
python3 "$CUR_DIR"/04512_send_header_and_column_defaults_for_insert.python 1 100
python3 "$CUR_DIR"/04512_send_header_and_column_defaults_for_insert.python 0 200

$CLICKHOUSE_CLIENT --query "SELECT * FROM test_04512 ORDER BY x"

$CLICKHOUSE_CLIENT --query "DROP TABLE test_04512"
