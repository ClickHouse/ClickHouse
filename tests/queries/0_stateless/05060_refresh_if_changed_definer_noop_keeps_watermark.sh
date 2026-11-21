#!/usr/bin/env bash
# Tags: no-ordinary-database, no-replicated-database
# Refreshable MVs with non-replicated inner tables are refused on a Replicated database.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -u

# Under `SQL SECURITY NONE` the refresh never reads the definer: `getSQLSecurityOverriddenContext`
# takes the `NONE` branch, which keeps the caller's settings and does not switch the user. So
# changing the definer of such a view is a semantic no-op, and it must not invalidate the
# `REFRESH ... IF CHANGED` watermark - an `APPEND` view would otherwise append a duplicate copy of
# unchanged rows.

definer="definer_05060_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -q "
    CREATE USER ${definer};
    CREATE TABLE src (x UInt64) ENGINE = MergeTree ORDER BY x;
    INSERT INTO src VALUES (1);
    -- APPEND mode: every refresh that actually runs appends one row.
    CREATE MATERIALIZED VIEW mv REFRESH EVERY 1 SECOND IF CHANGED APPEND
        ENGINE = MergeTree ORDER BY cnt
        DEFINER = ${definer} SQL SECURITY NONE AS SELECT count() AS cnt FROM src;
"

# The first refresh always runs: there is no previous state to compare to.
for _ in {1..60}
do
    initial=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM mv")
    [ "$initial" -ge 1 ] && break
    sleep 0.5
done

# A definer the view never reads changes. The source is unchanged, so the following scheduled
# refreshes must still be skipped.
$CLICKHOUSE_CLIENT -q "ALTER TABLE mv MODIFY DEFINER = CURRENT_USER SQL SECURITY NONE"

sleep 3
after=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM mv")
[ "$initial" = "1" ] && [ "$after" = "1" ] && echo "no-op definer change keeps the watermark: yes" || echo "no-op definer change keeps the watermark: no ($initial -> $after)"

# A real change of the source still triggers a refresh, so the watermark is in use rather than ignored.
$CLICKHOUSE_CLIENT -q "INSERT INTO src VALUES (2)"
for _ in {1..60}
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
