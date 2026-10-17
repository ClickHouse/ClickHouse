#!/usr/bin/env bash
# Tags: no-ordinary-database, no-replicated-database
# Refreshable MVs with non-replicated inner tables are refused on a Replicated database.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -u

# The `REFRESH ... IF CHANGED` watermark is keyed on a hash of the view definition, which folds the
# stored `SELECT`. The entries of its own `SETTINGS` clause that cannot change the produced rows are
# left out of it, so an `ALTER TABLE ... MODIFY QUERY` that only adds such settings keeps the
# watermark, and this `APPEND` view does not append a duplicate copy of unchanged rows. The same
# applies to the `modification_hash` of a plain view.

$CLICKHOUSE_CLIENT -q "
    CREATE TABLE src (x UInt64) ENGINE = MergeTree ORDER BY x;
    -- A merge would move the hash of the source table and make an extra refresh run.
    SYSTEM STOP MERGES src;
    INSERT INTO src VALUES (1);
    CREATE MATERIALIZED VIEW mv REFRESH EVERY 1 SECOND IF CHANGED APPEND
        ENGINE = MergeTree ORDER BY cnt AS SELECT count() AS cnt FROM src;
"

for _ in {1..60}
do
    initial=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM mv")
    [ "$initial" -ge 1 ] && break
    sleep 0.5
done

$CLICKHOUSE_CLIENT -q "ALTER TABLE mv MODIFY QUERY SELECT count() AS cnt FROM src SETTINGS use_query_cache = 1, log_comment = 'c_05333'"

sleep 3
after=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM mv")
[ "$initial" = "1" ] && [ "$after" = "1" ] && echo "operational query settings keep the watermark: yes" || echo "operational query settings keep the watermark: no ($initial -> $after)"

# A query setting that can change the rows still discards the watermark.
$CLICKHOUSE_CLIENT -q "ALTER TABLE mv MODIFY QUERY SELECT count() AS cnt FROM src SETTINGS use_query_cache = 1, max_threads = 1"
for _ in {1..60}
do
    invalidated=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM mv")
    [ "$invalidated" -ge 2 ] && break
    sleep 0.5
done
[ "$invalidated" -ge 2 ] && echo "a row-affecting query setting invalidates the watermark: yes" || echo "a row-affecting query setting invalidates the watermark: no ($invalidated)"

# The hash of a view folds its UUID, so recreate one view under the same UUID with another query.
uuid=$($CLICKHOUSE_CLIENT -q "SELECT generateUUIDv4()")
h()
{
    $CLICKHOUSE_CLIENT -q "
        CREATE VIEW v1 UUID '${uuid}' AS $1;
        SELECT modification_hash FROM system.tables WHERE database = currentDatabase() AND name = 'v1';
        DROP VIEW v1 SYNC;
    "
}
before=$(h "SELECT x FROM src")
operational=$(h "SELECT x FROM src SETTINGS use_query_cache = 1, log_comment = 'c_05333'")
row_affecting=$(h "SELECT x FROM src SETTINGS max_threads = 1")
[ -n "$before" ] && [ "$before" = "$operational" ] && echo "view hash ignores operational query settings: yes" || echo "view hash ignores operational query settings: no ($before, $operational)"
[ "$before" != "$row_affecting" ] && echo "view hash moves on a row-affecting query setting: yes" || echo "view hash moves on a row-affecting query setting: no"

$CLICKHOUSE_CLIENT -q "
    DROP TABLE mv SYNC;
    DROP TABLE src SYNC;
"
