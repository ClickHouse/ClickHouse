#!/usr/bin/env bash
# Tags: zookeeper, no-fasttest, no-ordinary-database
# Tag no-fasttest: needs the S3 mock (s3_conn named collection).
# Regression test for the `REFRESH ... IF CHANGED` watermark surviving the loss of in-memory task
# state (server restart / replica takeover), simulated here with DETACH DATABASE + ATTACH DATABASE.
# In coordinated mode (Replicated database) the watermark is persisted in the Keeper coordination
# znode. Before the fix it was a plain in-memory member, so after reattach the next scheduled
# refresh of an APPEND view re-ran and appended a duplicate row even though the source was
# unchanged.
# The source is an S3 table: its modification hash is built from the objects' strong `ETag`s, which
# are stable across a table reload (unlike local engines such as `MergeTree`, whose hashes fold
# process-lifetime loop-free counters and deliberately never survive a reload - the safe direction).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB="rdb_$CLICKHOUSE_DATABASE"
FILE="04870_${CLICKHOUSE_DATABASE}.csv"

$CLICKHOUSE_CLIENT -q "DROP DATABASE IF EXISTS $DB SYNC"
$CLICKHOUSE_CLIENT -q "CREATE DATABASE $DB ENGINE = Replicated('/clickhouse/databases/04870/$CLICKHOUSE_DATABASE', 'shard1', 'replica1')"

# Create the source object and an S3 table over it (single key, no glob, so the table exposes an
# ETag-based modification hash).
$CLICKHOUSE_CLIENT -q "INSERT INTO FUNCTION s3(s3_conn, filename='$FILE', format='CSV', structure='x UInt64') SETTINGS s3_truncate_on_insert = 1 VALUES (1)"
$CLICKHOUSE_CLIENT --distributed_ddl_output_mode=none -q "
    CREATE TABLE $DB.src (x UInt64) ENGINE = S3(s3_conn, filename='$FILE', format='CSV');
"
# APPEND mode: every refresh that actually runs appends one row to the view. The view is in a
# Replicated database, so the refresh is coordinated and its state lives in Keeper.
$CLICKHOUSE_CLIENT --distributed_ddl_output_mode=none -q "
    CREATE MATERIALIZED VIEW $DB.mv REFRESH EVERY 1 SECOND IF CHANGED APPEND
        ENGINE = ReplicatedMergeTree ORDER BY cnt AS SELECT count() AS cnt FROM $DB.src;
"

# Wait for the first refresh to run (it always runs, since there is no previous state to compare to).
for _ in {1..120}
do
    n=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM $DB.mv")
    [ "$n" -ge 1 ] && break
    sleep 0.5
done

# The source is unchanged, so the next scheduled refreshes are skipped; this also gives the finished
# refresh time to write its watermark to the coordination znode in Keeper.
sleep 3
n2=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM $DB.mv")
[ "$n2" = "1" ] && echo "unchanged stays one: yes" || echo "unchanged stays one: no ($n2)"

# Drop all in-memory refresh state. On reattach the watermark must come back from Keeper, so the
# next scheduled refreshes keep being skipped. Before the fix the watermark was lost here, and the
# first post-reattach refresh appended a duplicate row.
$CLICKHOUSE_CLIENT -q "DETACH DATABASE $DB"
t0=$($CLICKHOUSE_CLIENT -q "SELECT toUnixTimestamp(now())")
$CLICKHOUSE_CLIENT -q "ATTACH DATABASE $DB"

# Wait for at least one post-reattach refresh attempt (`last_refresh_time` advances on every
# attempt, including skipped ones), so the check below is not vacuously green while the reattached
# view is still loading.
for _ in {1..120}
do
    attempted=$($CLICKHOUSE_CLIENT -q "
        SELECT count() FROM system.view_refreshes
        WHERE database = '$DB' AND view = 'mv'
            AND last_refresh_time IS NOT NULL AND toUnixTimestamp(last_refresh_time) >= $t0")
    [ "$attempted" -ge 1 ] && break
    sleep 0.5
done

n3=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM $DB.mv")
[ "$n3" = "1" ] && echo "watermark survives reattach: yes" || echo "watermark survives reattach: no ($n3)"

# Change the source object (new ETag). A scheduled refresh must now run again and append one more row.
$CLICKHOUSE_CLIENT -q "INSERT INTO FUNCTION s3(s3_conn, filename='$FILE', format='CSV', structure='x UInt64') SETTINGS s3_truncate_on_insert = 1 VALUES (1), (2)"
for _ in {1..120}
do
    n4=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM $DB.mv")
    [ "$n4" -ge 2 ] && break
    sleep 0.5
done
[ "$n4" -ge 2 ] && echo "changed triggers refresh: yes" || echo "changed triggers refresh: no ($n4)"

$CLICKHOUSE_CLIENT -q "DROP DATABASE $DB SYNC"
