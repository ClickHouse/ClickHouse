#!/usr/bin/env bash
# Tags: no-ordinary-database, no-replicated-database
# Refreshable MVs with non-replicated inner tables are refused on a Replicated database.
# The `REFRESH ... IF CHANGED` watermark folds in the view's SELECT query identity (it is no longer
# reset in-memory on ALTER, because it is stored in the coordination state), so a watermark recorded
# under the old query must never make the first scheduled refresh after `ALTER ... MODIFY QUERY` be
# skipped: the source tables are unchanged here, and only the query fold forces the rebuild.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS src SYNC;
    DROP TABLE IF EXISTS mv SYNC;
    CREATE TABLE src (x UInt64) ENGINE = MergeTree ORDER BY x;
    INSERT INTO src VALUES (1);
    CREATE MATERIALIZED VIEW mv REFRESH EVERY 1 SECOND IF CHANGED
        ENGINE = MergeTree ORDER BY cnt AS SELECT count() AS cnt FROM src;
"

# Wait for the first refresh to run and record the watermark.
for _ in {1..120}
do
    n=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM mv")
    [ "$n" -ge 1 ] && break
    sleep 0.5
done
v=$($CLICKHOUSE_CLIENT -q "SELECT cnt FROM mv")
[ "$v" = "1" ] && echo "initial refresh: yes" || echo "initial refresh: no ($v)"

# Let a couple of scheduled refreshes be skipped while the source is unchanged.
sleep 2

# Change the query without changing the source. The recorded watermark refers to the old query, so
# the next scheduled refresh must run and rebuild the view under the new query.
$CLICKHOUSE_CLIENT -q "ALTER TABLE mv MODIFY QUERY SELECT count() + 100 AS cnt FROM src"

for _ in {1..120}
do
    v2=$($CLICKHOUSE_CLIENT -q "SELECT max(cnt) FROM mv")
    [ "$v2" = "101" ] && break
    sleep 0.5
done
[ "$v2" = "101" ] && echo "modify query triggers refresh: yes" || echo "modify query triggers refresh: no ($v2)"

$CLICKHOUSE_CLIENT -q "
    DROP TABLE mv SYNC;
    DROP TABLE src SYNC;
"
