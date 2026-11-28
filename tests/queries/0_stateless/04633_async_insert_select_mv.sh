#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# An async `INSERT ... SELECT` into a table with dependent materialized views reaches the queue if
# every view target is a `MergeTree`-family table. A view may read other tables and the SELECT may read
# a table of the graph: the query releases its locks before it waits for the flush. Otherwise it runs
# synchronously and still feeds the views.
${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE side (n UInt64) ENGINE = MergeTree ORDER BY n;
    INSERT INTO side SETTINGS async_insert = 0 VALUES (1), (2);

    CREATE TABLE src (n UInt64) ENGINE = MergeTree ORDER BY n;
    CREATE TABLE t1 (n UInt64) ENGINE = MergeTree ORDER BY n;
    CREATE TABLE t2 (n UInt64) ENGINE = MergeTree ORDER BY n;
    CREATE MATERIALIZED VIEW mv1 TO t1 AS SELECT n * 10 AS n FROM src;
    CREATE MATERIALIZED VIEW mv2 TO t2 AS SELECT n + 1 AS n FROM t1;

    CREATE TABLE src_memory (n UInt64) ENGINE = MergeTree ORDER BY n;
    CREATE TABLE memory_target (n UInt64) ENGINE = Memory;
    CREATE MATERIALIZED VIEW mv_memory TO memory_target AS SELECT n FROM src_memory;

    CREATE TABLE src_join (n UInt64) ENGINE = MergeTree ORDER BY n;
    CREATE TABLE join_target (n UInt64) ENGINE = MergeTree ORDER BY n;
    CREATE MATERIALIZED VIEW mv_join TO join_target AS SELECT n FROM src_join INNER JOIN side USING (n);

    CREATE TABLE jt (n UInt64, v UInt64) ENGINE = Join(ANY, LEFT, n);
    INSERT INTO jt SETTINGS async_insert = 0 VALUES (1, 10), (2, 20);
    CREATE TABLE src_joinget (v UInt64) ENGINE = MergeTree ORDER BY v;
"

# Pinned per query, so the randomizer cannot split the block into several.
SETTINGS="async_insert = 1, wait_for_async_insert = 1, max_threads = 1, max_insert_threads = 1,
    max_block_size = 1000, preferred_block_size_bytes = 0"

function insert() {
	${CLICKHOUSE_CLIENT} --query_id "${CLICKHOUSE_DATABASE}_04633_mv_$1" -q "$2 SETTINGS $SETTINGS"
}

insert chain "INSERT INTO src SELECT number FROM numbers(3)"
${CLICKHOUSE_CLIENT} -q "
    SELECT 'src', arraySort(groupArray(n)) FROM src;
    SELECT 't1', arraySort(groupArray(n)) FROM t1;
    SELECT 't2', arraySort(groupArray(n)) FROM t2;
"

insert memory "INSERT INTO src_memory SELECT number FROM numbers(3)"
${CLICKHOUSE_CLIENT} -q "SELECT 'memory_target', count() FROM memory_target"

insert join "INSERT INTO src_join SELECT number FROM numbers(3)"
${CLICKHOUSE_CLIENT} -q "SELECT 'join_target', count() FROM join_target"

insert reads_graph "INSERT INTO src SELECT n FROM t1"
${CLICKHOUSE_CLIENT} -q "SELECT 'src', count() FROM src"

# `joinGet` keeps its lock on `jt` in the SELECT pipeline, which is gone before the block is pushed.
insert joinget "INSERT INTO src_joinget SELECT joinGet('jt', 'v', number) FROM numbers(3)"
${CLICKHOUSE_CLIENT} -q "SELECT 'src_joinget', arraySort(groupArray(v)) FROM src_joinget"

function queued() {
	${CLICKHOUSE_CLIENT} -q "
        SELECT count() >= 1
        FROM system.asynchronous_insert_log
        WHERE event_date >= yesterday() AND event_time >= now() - 600
          AND database = currentDatabase()
          AND query_id = '${CLICKHOUSE_DATABASE}_04633_mv_$1'
    "
}

# Log elements are queued after the query returns, so one flush can miss them:
# https://github.com/ClickHouse/ClickHouse/issues/84364. Retry bounded so a real failure prints 0.
# Filtered by query_id: the AsyncInsert CI profile forces async_insert = 1 for the seed insert too.
reached_queue=0
for _ in $(seq 1 60); do
	${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS asynchronous_insert_log"
	reached_queue=$(queued chain)
	[ "$reached_queue" = 1 ] && break
	sleep 0.5
done
echo "chain queued: $reached_queue"
for name in memory join reads_graph joinget; do
	echo "$name queued: $(queued "$name")"
done

${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS query_views_log"
${CLICKHOUSE_CLIENT} -q "
    SELECT replaceOne(view_name, currentDatabase() || '.', ''), status
    FROM system.query_views_log
    WHERE event_date >= yesterday()
      AND initial_query_id = (
          SELECT flush_query_id FROM system.asynchronous_insert_log
          WHERE event_date >= yesterday() AND database = currentDatabase()
            AND query_id = '${CLICKHOUSE_DATABASE}_04633_mv_chain')
    ORDER BY view_name
"
