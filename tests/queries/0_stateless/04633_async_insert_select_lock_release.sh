#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: every case sleeps for about two seconds.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A writer queued behind a lock the SELECT holds must not deadlock with the queue flush, which locks
# the same table under its own query id: the block is pushed only once the SELECT pipeline, with every
# lock it holds, is gone.
# `POPULATE` takes the write lock on its source; its short timeout bounds what a deadlock costs.
# Local disk: a write-through filesystem cache can stall the flush's part write past that timeout.

SETTINGS=(--async_insert=1 --wait_for_async_insert=1 --max_threads=1 --max_insert_threads=1
	--max_block_size=10000 --preferred_block_size_bytes=0 --optimize_trivial_insert_select=0
	--function_sleep_max_microseconds_per_block=3000000)

function race() {
	local name="$1" locked_table="$2" insert="$3"
	local query_id="${CLICKHOUSE_DATABASE}_${name}"

	${CLICKHOUSE_CLIENT} "${SETTINGS[@]}" --query_id "$query_id" -q "$insert" &
	local pid=$!
	wait_for_query_to_start "$query_id" 30 "$pid"
	${CLICKHOUSE_CLIENT} --lock_acquire_timeout=10 -q "
        CREATE MATERIALIZED VIEW populate_${name}
        ENGINE = MergeTree ORDER BY a SETTINGS storage_policy = 'default' POPULATE
        AS SELECT a FROM ${locked_table}
    "
	wait "$pid"

	local reached_queue=0
	for _ in $(seq 1 60); do
		${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS asynchronous_insert_log"
		reached_queue=$(${CLICKHOUSE_CLIENT} -q "
            SELECT count() >= 1
            FROM system.asynchronous_insert_log
            WHERE event_date >= yesterday() AND event_time >= now() - 600
              AND database = currentDatabase()
              AND query_id = '$query_id'
        ")
		[ "$reached_queue" = 1 ] && break
		sleep 0.5
	done
	echo "$name queued: $reached_queue"
}

${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE self (a UInt32) ENGINE = MergeTree ORDER BY a SETTINGS storage_policy = 'default';
    INSERT INTO self SETTINGS async_insert = 0 SELECT number FROM numbers(2000);

    CREATE TABLE src (a UInt32) ENGINE = MergeTree ORDER BY a SETTINGS storage_policy = 'default';
    CREATE TABLE dst (a UInt32) ENGINE = MergeTree ORDER BY a SETTINGS storage_policy = 'default';
    CREATE MATERIALIZED VIEW mv TO dst AS SELECT a FROM src;
    INSERT INTO dst SETTINGS async_insert = 0 SELECT number FROM numbers(2000);

    CREATE TABLE subquery (a UInt32) ENGINE = MergeTree ORDER BY a SETTINGS storage_policy = 'default';
    INSERT INTO subquery SETTINGS async_insert = 0 SELECT number FROM numbers(2000);

    CREATE TABLE side (a UInt32) ENGINE = MergeTree ORDER BY a SETTINGS storage_policy = 'default';
    INSERT INTO side SETTINGS async_insert = 0 SELECT number FROM numbers(2000);
    CREATE TABLE side_src (a UInt32) ENGINE = MergeTree ORDER BY a SETTINGS storage_policy = 'default';
    CREATE TABLE side_dst (a UInt32) ENGINE = MergeTree ORDER BY a SETTINGS storage_policy = 'default';
    CREATE MATERIALIZED VIEW side_mv TO side_dst AS SELECT a FROM side_src WHERE a IN side;
"

race self self "INSERT INTO self SELECT a FROM self WHERE sleepEachRow(0.001) = 0"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM self"

# The flush writes into `dst` through the view.
race view_target dst "INSERT INTO src SELECT a FROM dst WHERE sleepEachRow(0.001) = 0"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM src; SELECT count() FROM dst"

# The set of the subquery keeps its table locked until the SELECT pipeline ends.
race subquery subquery "
    INSERT INTO subquery SELECT number FROM numbers(2000)
    WHERE number IN (SELECT a FROM subquery) AND sleepEachRow(0.001) = 0
"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM subquery"

# The flush locks `side` for the view, which the SELECT reads too.
race view_reads side "INSERT INTO side_src SELECT a FROM side WHERE sleepEachRow(0.001) = 0"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM side_src; SELECT count() FROM side_dst"
