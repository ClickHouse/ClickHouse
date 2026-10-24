#!/usr/bin/env bash
# Tags: no-ordinary-database, no-replicated-database

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -u

# A plain view's `getViewContext` pins `max_result_rows` / `max_result_bytes` to zero, but a read of a
# `SQL SECURITY DEFINER` materialized view and a refresh run under the definer's result limits as
# they are. A result limit added to the definer's profile can truncate or fail them, so it must move
# the materialized view's `modification_hash` and invalidate a `REFRESH ... IF CHANGED` watermark.
# The limits used here are far above what the views return, so nothing fails and only the change of
# the definer's settings is observed.
# The limits are `CONST`: `getSQLSecurityOverriddenContext` applies the invoker's changed settings on
# top of the definer's profile, and the test server's default profile already sets both limits, so a
# plain definer value would be overridden and would rightly leave the hash unmoved.

# Users are server-wide, so make the name unique per run.
definer="definer_05331_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -q "
    CREATE USER ${definer};
    GRANT SELECT, INSERT ON ${CLICKHOUSE_DATABASE}.* TO ${definer};

    CREATE TABLE src (x UInt64) ENGINE = MergeTree ORDER BY x;
    CREATE TABLE target (x UInt64) ENGINE = MergeTree ORDER BY x;
    INSERT INTO src VALUES (1), (2);
    INSERT INTO target VALUES (1), (2);

    CREATE MATERIALIZED VIEW mv TO target (x UInt64)
        DEFINER = ${definer} SQL SECURITY DEFINER AS SELECT x FROM src;
"

hash_of_mv()
{
    $CLICKHOUSE_CLIENT -q "
        SELECT toString(modification_hash)
        FROM system.tables
        WHERE database = currentDatabase() AND name = 'mv'
    "
}

baseline=$(hash_of_mv)
[ -n "${baseline}" ] && echo 'hash is computed'

$CLICKHOUSE_CLIENT -q "ALTER USER ${definer} SETTINGS max_result_rows = 1000 CONST"
[ "${baseline}" != "$(hash_of_mv)" ] && echo 'max_result_rows in the definer profile changes the hash'

$CLICKHOUSE_CLIENT -q "ALTER USER ${definer} SETTINGS max_result_bytes = 1000000 CONST"
[ "${baseline}" != "$(hash_of_mv)" ] && echo 'max_result_bytes in the definer profile changes the hash'

$CLICKHOUSE_CLIENT -q "ALTER USER ${definer} SETTINGS NONE"
[ "${baseline}" = "$(hash_of_mv)" ] && echo 'the hash is back after the limits are dropped'

# The same for a refresh: the source data does not change, but the definer's result limit does, so
# the next `IF CHANGED APPEND` refresh must run instead of being skipped.
$CLICKHOUSE_CLIENT -q "
    CREATE MATERIALIZED VIEW rmv REFRESH EVERY 1 SECOND IF CHANGED APPEND
        ENGINE = MergeTree ORDER BY x
        DEFINER = ${definer} SQL SECURITY DEFINER AS SELECT x FROM src;
"

# Keep the two polling phases comfortably below the Fast test's 60-second timeout.
for _ in {1..30}
do
    initial=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM rmv")
    [ "$initial" -eq 2 ] && break
    sleep 0.5
done

$CLICKHOUSE_CLIENT -q "ALTER USER ${definer} SETTINGS max_result_rows = 1000 CONST"

for _ in {1..30}
do
    rows=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM rmv")
    [ "$rows" -eq 4 ] && break
    sleep 0.5
done

[ "$initial" -eq 2 ] && [ "$rows" -eq 4 ] && echo "a definer result limit invalidates the watermark: yes" || echo "a definer result limit invalidates the watermark: no ($initial -> $rows)"

# The views first: a user cannot be dropped while it is the definer of one.
$CLICKHOUSE_CLIENT -q "
    DROP TABLE rmv SYNC;
    DROP TABLE mv;
    DROP TABLE target;
    DROP TABLE src SYNC;
    DROP USER ${definer};
"
