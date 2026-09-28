#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A definition the user supplies now is validated the same way whether the statement is executed
# directly or through a wrapper that runs it as an internal query, such as `PARALLEL WITH`. Keying
# the compatibility clamp on `internal` alone let such a `CREATE` through: the metadata file then held
# the value as written while the database used `UINT32_MAX` in memory and in Keeper.
#
# The partner statement has neither input nor output, which is what `PARALLEL WITH` requires.
#
# Every case starts from a clean slate, so that a rejection that fails to happen does not turn the
# next case into `DATABASE_ALREADY_EXISTS`.

db="${CLICKHOUSE_DATABASE}_wrapper"
zk_path="/test/${CLICKHOUSE_DATABASE}/05223"

$CLICKHOUSE_CLIENT -q "DROP DATABASE IF EXISTS $db SYNC"

echo -n "create: "
$CLICKHOUSE_CLIENT -q "
    CREATE DATABASE $db ENGINE = Replicated('$zk_path', 's1', 'r1')
    SETTINGS logs_to_keep = 10000000000
    PARALLEL WITH
    DROP TABLE IF EXISTS $CLICKHOUSE_DATABASE.no_such_table" 2>&1 \
    | grep -o -m 1 -F "BAD_ARGUMENTS"

# The rejection happens before anything is registered.
$CLICKHOUSE_CLIENT -q "SELECT count() FROM system.databases WHERE name = '$db'"

# A full-syntax `ATTACH` carries a user-written definition too, and with no metadata file on disk it
# would become the definition of record, so it is rejected the same way. The UUID is generated rather
# than written out: it is what the server would store the database under, so a fixed one collides
# with a concurrent run of this test.
uuid=$($CLICKHOUSE_CLIENT -q "SELECT generateUUIDv4()")

$CLICKHOUSE_CLIENT -q "DROP DATABASE IF EXISTS $db SYNC"

echo -n "attach: "
$CLICKHOUSE_CLIENT -q "
    ATTACH DATABASE $db UUID '$uuid' ENGINE = Replicated('$zk_path', 's1', 'r1')
    SETTINGS logs_to_keep = 10000000000
    PARALLEL WITH
    DROP TABLE IF EXISTS $CLICKHOUSE_DATABASE.no_such_table" 2>&1 \
    | grep -o -m 1 -F "BAD_ARGUMENTS"

$CLICKHOUSE_CLIENT -q "SELECT count() FROM system.databases WHERE name = '$db'"

$CLICKHOUSE_CLIENT -q "DROP DATABASE IF EXISTS $db SYNC"
