#!/usr/bin/env bash
# Tags: zookeeper, no-fasttest, no-ordinary-database

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `REFRESH ... IF CHANGED` persists its watermark for coordinated refreshes. The watermark's
# definition hash must include the materialized view's SQL-security context: changing it can change
# the query's permissions, row policies, and constrained settings even when the source is unchanged.

DB="rdb_$CLICKHOUSE_DATABASE"

$CLICKHOUSE_CLIENT -q "
    DROP DATABASE IF EXISTS $DB SYNC;
    CREATE DATABASE $DB ENGINE = Replicated('/clickhouse/databases/04907/$CLICKHOUSE_DATABASE', 'shard1', 'replica1');
"

$CLICKHOUSE_CLIENT --distributed_ddl_output_mode=none -q "
    CREATE TABLE $DB.src (x UInt64) ENGINE = MergeTree ORDER BY x;
    INSERT INTO $DB.src VALUES (1);
    CREATE MATERIALIZED VIEW $DB.mv REFRESH EVERY 1 SECOND IF CHANGED APPEND
        ENGINE = ReplicatedMergeTree ORDER BY cnt
        DEFINER = CURRENT_USER SQL SECURITY DEFINER AS SELECT count() AS cnt FROM $DB.src;
"

# The initial refresh records a watermark. Subsequent refreshes over the unchanged source are skipped.
for _ in {1..120}
do
    rows=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM $DB.mv")
    [ "$rows" = "1" ] && break
    sleep 0.5
done
[ "$rows" = "1" ] && echo "initial refresh: yes" || echo "initial refresh: no ($rows)"

sleep 2
before=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM $DB.mv")

# No source data changes. This changes only the refresh's execution security context, so the next
# refresh must rebuild instead of reusing the persisted watermark from the DEFINER context.
$CLICKHOUSE_CLIENT --distributed_ddl_output_mode=none -q "ALTER TABLE $DB.mv MODIFY SQL SECURITY NONE"

for _ in {1..120}
do
    after=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM $DB.mv")
    [ "$after" -gt "$before" ] && break
    sleep 0.5
done
[ "$after" -gt "$before" ] && echo "modify SQL security triggers refresh: yes" || echo "modify SQL security triggers refresh: no ($before -> $after)"

$CLICKHOUSE_CLIENT -q "DROP DATABASE $DB SYNC"
