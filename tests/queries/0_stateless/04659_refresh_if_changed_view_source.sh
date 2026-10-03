#!/usr/bin/env bash
# Tags: no-ordinary-database, no-replicated-database
# Refreshable MVs with non-replicated inner tables are refused on a Replicated database.
# Functional test for REFRESH ... IF CHANGED reading through a view (issue #108713): the view has no
# data of its own, so the source hash must be derived from the table behind the view's stored SELECT.
# Scheduled refreshes are skipped while that table is unchanged, and run again once it changes.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS mv SYNC;
    DROP TABLE IF EXISTS v;
    DROP TABLE IF EXISTS src SYNC;
    CREATE TABLE src (x UInt64) ENGINE = MergeTree ORDER BY x;
    INSERT INTO src VALUES (1);
    CREATE VIEW v AS SELECT x FROM src;
    -- APPEND mode: every refresh that actually runs appends one row to the view.
    CREATE MATERIALIZED VIEW mv REFRESH EVERY 1 SECOND IF CHANGED APPEND
        ENGINE = MergeTree ORDER BY cnt AS SELECT count() AS cnt FROM v;
"

# Wait for the first refresh to run (it always runs, since there is no previous state to compare to).
for _ in {1..120}
do
    n=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM mv")
    [ "$n" -ge 1 ] && break
    sleep 0.5
done

# The table behind the view is unchanged, so the next several scheduled refreshes must be skipped:
# the row count stays put.
sleep 3
n2=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM mv")
[ "$n2" = "1" ] && echo "unchanged stays one: yes" || echo "unchanged stays one: no ($n2)"

# Change the table behind the view. A scheduled refresh must now run again and append one more row.
$CLICKHOUSE_CLIENT -q "INSERT INTO src VALUES (2)"
for _ in {1..120}
do
    n3=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM mv")
    [ "$n3" -ge 2 ] && break
    sleep 0.5
done
[ "$n3" -ge 2 ] && echo "changed triggers refresh: yes" || echo "changed triggers refresh: no ($n3)"

$CLICKHOUSE_CLIENT -q "
    DROP TABLE mv SYNC;
    DROP TABLE v;
    DROP TABLE src SYNC;
"
