#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS test_arrow_uuid"
$CLICKHOUSE_CLIENT -q "CREATE TABLE test_arrow_uuid (id UUID) ENGINE = Memory"

$CLICKHOUSE_CLIENT -q "INSERT INTO test_arrow_uuid VALUES ('5056c67a-4297-431a-9941-839d04eaa2d7'), ('3aa723be-5dad-4323-b898-41e5e13bc439'), ('ffffffff-ffff-ffff-ffff-ffffffffffff')"

# Export from the server via Arrow format, pipe the binary output directly 
# into clickhouse-local, and ask it to parse the Arrow bytes back into UUIDs.
$CLICKHOUSE_CLIENT -q "SELECT * FROM test_arrow_uuid FORMAT Arrow" | \
    $CLICKHOUSE_LOCAL --input-format Arrow --structure "id UUID" -q "SELECT * FROM table"

$CLICKHOUSE_CLIENT -q "DROP TABLE test_arrow_uuid"
