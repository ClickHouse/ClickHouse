#!/usr/bin/env bash
# Tags: no-parallel
# `SYSTEM START VIEWS` has no table argument, so it would start refreshable views created by
# concurrently running tests.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A `CREATE OR REPLACE` of a refreshable materialized view runs the replacement view under a
# temporary `_tmp_replace_*` name until the rename commits, and the refresher is held back until
# then so it cannot write to the target table early. `SYSTEM START VIEWS` (and `SYSTEM START ALL
# BACKGROUND`) call into every table in every database without checking whether they ever stopped
# it, so they must not release that hold.

function cleanup()
{
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT create_or_replace_before_rename" 2>/dev/null ||:
    $CLICKHOUSE_CLIENT -q "SYSTEM STOP VIEW v" 2>/dev/null ||:
}
trap cleanup EXIT

$CLICKHOUSE_CLIENT -q "CREATE TABLE src (x Int64) ENGINE = Memory"
$CLICKHOUSE_CLIENT -q "INSERT INTO src VALUES (1)"
$CLICKHOUSE_CLIENT -q "CREATE TABLE dest (x Int64) ENGINE = Memory"

# The original view fills the shared target once, then stays out of the way.
$CLICKHOUSE_CLIENT -q "CREATE MATERIALIZED VIEW v REFRESH EVERY 1 YEAR TO dest AS SELECT x FROM src"
$CLICKHOUSE_CLIENT -q "SYSTEM WAIT VIEW v"
$CLICKHOUSE_CLIENT -q "SYSTEM STOP VIEW v"
$CLICKHOUSE_CLIENT -q "SELECT 'before', groupArray(x) FROM dest"

$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT create_or_replace_before_rename"

# Pauses with the replacement view registered under its temporary name, before the rename.
$CLICKHOUSE_CLIENT -q "CREATE OR REPLACE MATERIALIZED VIEW v REFRESH EVERY 1 SECOND TO dest AS SELECT x * 10 AS x FROM src" &
CREATE_PID=$!

$CLICKHOUSE_CLIENT -q "SYSTEM WAIT FAILPOINT create_or_replace_before_rename PAUSE"

$CLICKHOUSE_CLIENT -q "SYSTEM START VIEWS"

# The replacement view refreshes every second, so a second is enough for it to write to `dest` if
# the hold was released.
sleep 2

# It must still be held back, and `dest` must still hold what the original view wrote.
$CLICKHOUSE_CLIENT -q "
    SELECT 'held back', countIf(status = 'Disabled'), count() FROM system.view_refreshes
        WHERE database = currentDatabase() AND view LIKE '_tmp_replace%';
    SELECT 'target untouched', groupArray(x) FROM dest;"

$CLICKHOUSE_CLIENT -q "SYSTEM NOTIFY FAILPOINT create_or_replace_before_rename"
wait $CREATE_PID

# Once the rename committed, the replacement view refreshes normally.
$CLICKHOUSE_CLIENT -q "SYSTEM WAIT VIEW v"
$CLICKHOUSE_CLIENT -q "SELECT 'after commit', groupArray(x) FROM dest"

$CLICKHOUSE_CLIENT -q "SYSTEM STOP VIEW v"
$CLICKHOUSE_CLIENT -q "DROP TABLE v"
$CLICKHOUSE_CLIENT -q "DROP TABLE dest"
$CLICKHOUSE_CLIENT -q "DROP TABLE src"
