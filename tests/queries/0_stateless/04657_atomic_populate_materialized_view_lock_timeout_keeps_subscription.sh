#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# An atomic POPULATE takes a brief exclusive lock on the source table. If that lock acquisition times
# out (the source is busy), the CREATE fails - but the view has already been created and started, so
# the failure path must still register the view's dependency on the source. Otherwise the view would
# exist but never receive live pushes. This test makes the exclusive lock time out by holding a shared
# lock with a slow concurrent INSERT, and then checks that the view still receives new inserts.

$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS src_lock_timeout;
    DROP TABLE IF EXISTS mv_lock_timeout;
    CREATE TABLE src_lock_timeout (x UInt64) ENGINE = MergeTree ORDER BY x;
    INSERT INTO src_lock_timeout VALUES (1);
"

# A slow INSERT holds a shared lock on the source for its whole duration (~5 s).
$CLICKHOUSE_CLIENT --max_block_size 1 --min_insert_block_size_rows 1 --query "
    INSERT INTO src_lock_timeout SELECT number + 100 + ignore(sleepEachRow(0.1)) FROM numbers(50)" &
insert_pid=$!

# Each block of the slow INSERT is committed as soon as it is written, so a second visible row proves
# the INSERT is executing and therefore holds its shared lock (it is held for the whole query).
for _ in {1..600}
do
    count=$($CLICKHOUSE_CLIENT --query "SELECT count() FROM src_lock_timeout")
    [ "$count" -ge 2 ] && break
    sleep 0.1
done

# The exclusive lock inside the atomic POPULATE cannot be acquired while the INSERT is running.
$CLICKHOUSE_CLIENT --lock_acquire_timeout 1 --query "
    CREATE MATERIALIZED VIEW mv_lock_timeout ENGINE = MergeTree ORDER BY x POPULATE AS SELECT x FROM src_lock_timeout" 2>&1 \
    | grep -o -m1 'DEADLOCK_AVOIDED'

wait "$insert_pid"

# The view exists, is empty (the population failed), but is subscribed: a new insert reaches it.
$CLICKHOUSE_CLIENT --query "EXISTS TABLE mv_lock_timeout"
$CLICKHOUSE_CLIENT --query "SELECT count() FROM mv_lock_timeout"
$CLICKHOUSE_CLIENT --query "INSERT INTO src_lock_timeout VALUES (1000)"
$CLICKHOUSE_CLIENT --query "SELECT count() FROM mv_lock_timeout"

$CLICKHOUSE_CLIENT --query "
    DROP TABLE mv_lock_timeout;
    DROP TABLE src_lock_timeout;
"
