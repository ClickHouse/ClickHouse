#!/usr/bin/env bash
# Tags: no-ordinary-database, no-replicated-database
# Refreshable MVs with non-replicated inner tables are refused on a Replicated database.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -u

# The refresh of a `SQL SECURITY DEFINER` view runs with the definer's settings, and then with the
# stored query's own `SETTINGS` on top of them. A change in the definer's profile of a setting that
# the stored query overrides anyway does not change what the refresh produces, so it must not
# invalidate the `REFRESH ... IF CHANGED` watermark - an `APPEND` view would otherwise append a
# duplicate copy of unchanged rows. All values are far above what the refresh reads.
definer="definer_${CLICKHOUSE_TEST_UNIQUE_NAME}"

$CLICKHOUSE_CLIENT -q "
    CREATE USER ${definer} SETTINGS max_rows_to_read_leaf = 1000;
    GRANT SELECT, INSERT ON ${CLICKHOUSE_DATABASE}.* TO ${definer};
    CREATE TABLE src (x UInt64) ENGINE = MergeTree ORDER BY x;
    INSERT INTO src VALUES (1);
    -- APPEND mode: every refresh that actually runs appends one row.
    CREATE MATERIALIZED VIEW mv REFRESH EVERY 1 SECOND IF CHANGED APPEND
        ENGINE = MergeTree ORDER BY cnt
        DEFINER = ${definer} SQL SECURITY DEFINER
        AS SELECT count() AS cnt FROM src SETTINGS max_rows_to_read_leaf = 100000;
"

# The first refresh always runs: there is no previous state to compare to.
for _ in {1..30}
do
    initial=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM mv")
    [ "$initial" -ge 1 ] && break
    sleep 0.5
done

$CLICKHOUSE_CLIENT -q "ALTER USER ${definer} SETTINGS max_rows_to_read_leaf = 2000"

sleep 3
after=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM mv")
[ "$initial" = "1" ] && [ "$after" = "1" ] && echo "overridden definer setting keeps the watermark: yes" || echo "overridden definer setting keeps the watermark: no ($initial -> $after)"

# A real change of the source still triggers a refresh, so the watermark is in use rather than ignored.
$CLICKHOUSE_CLIENT -q "INSERT INTO src VALUES (2)"
for _ in {1..30}
do
    changed=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM mv")
    [ "$changed" -ge 2 ] && break
    sleep 0.5
done
[ "$changed" -ge 2 ] && echo "changed source triggers refresh: yes" || echo "changed source triggers refresh: no ($changed)"

$CLICKHOUSE_CLIENT -q "
    DROP TABLE mv SYNC;
    DROP TABLE src SYNC;
    DROP USER ${definer};
"
