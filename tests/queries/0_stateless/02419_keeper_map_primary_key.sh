#!/usr/bin/env bash
# Tags: no-ordinary-database, no-fasttest, long

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS 02419_test SYNC;"

test_primary_key()
{
    $CLICKHOUSE_CLIENT -m -q "
    CREATE TABLE 02419_test (key UInt64, value Float64) Engine=KeeperMap('/' || currentDatabase() || '/test2418', 3) PRIMARY KEY($1);
    INSERT INTO 02419_test VALUES (1, 1.1), (2, 2.2);
    SELECT value FROM 02419_test WHERE key = 1;
    SELECT value FROM 02419_test WHERE key IN (2, 3);
    DROP TABLE 02419_test SYNC;
    "
}

test_primary_key "sipHash64(key + 42) * 12212121212121"
test_primary_key "reverse(concat(CAST(key, 'String'), 'some string'))"
test_primary_key "hex(toFloat32(key))"
