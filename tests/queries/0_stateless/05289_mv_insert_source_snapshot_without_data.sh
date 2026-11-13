#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The SELECT of a materialized view is executed over the inserted block, not over the source table.
# The source table must not have its data snapshot (all active parts for MergeTree) taken for every
# block and every view. `merge_tree_storage_snapshot_sleep_ms` logs a message each time a data snapshot
# is taken, so the number of such messages attributed to the insert must be zero.

$CLICKHOUSE_CLIENT -nm --query "
DROP TABLE IF EXISTS mv1;
DROP TABLE IF EXISTS mv2;
DROP TABLE IF EXISTS src;
DROP TABLE IF EXISTS dst1;
DROP TABLE IF EXISTS dst2;

CREATE TABLE src (id UInt64, v String) ENGINE = MergeTree ORDER BY id;
CREATE TABLE dst1 (id UInt64, v String) ENGINE = MergeTree ORDER BY id;
CREATE TABLE dst2 (id UInt64, v String) ENGINE = MergeTree ORDER BY id;

INSERT INTO src VALUES (0, 'existing');

CREATE MATERIALIZED VIEW mv1 TO dst1 AS SELECT id, v FROM src;
CREATE MATERIALIZED VIEW mv2 TO dst2 AS SELECT id, v FROM src WHERE id IN (SELECT id FROM src);
"

sync_query_id="${CLICKHOUSE_DATABASE}_sync_insert"
$CLICKHOUSE_CLIENT --query_id="$sync_query_id" --async_insert=0 --merge_tree_storage_snapshot_sleep_ms=1 --query "INSERT INTO src VALUES (1, 'sync')"

async_query_id="${CLICKHOUSE_DATABASE}_async_insert"
$CLICKHOUSE_CLIENT --query_id="$async_query_id" --async_insert=1 --wait_for_async_insert=1 --merge_tree_storage_snapshot_sleep_ms=1 --query "INSERT INTO src VALUES (2, 'async')"

$CLICKHOUSE_CLIENT -nm --query "
SELECT 'dst1', * FROM dst1 ORDER BY id;
SELECT 'dst2', * FROM dst2 ORDER BY id;

SYSTEM FLUSH LOGS text_log, asynchronous_insert_log;

SELECT 'snapshots taken by sync insert', count()
FROM system.text_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
    AND query_id = '$sync_query_id'
    AND message_format_string = 'Injecting {}ms artificial delay before taking storage snapshot'
SETTINGS max_rows_to_read = 0;

SELECT 'snapshots taken by async insert flush', count()
FROM system.text_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
    AND query_id IN (
        SELECT flush_query_id FROM system.asynchronous_insert_log
        WHERE event_date >= yesterday() AND query_id = '$async_query_id')
    AND message_format_string = 'Injecting {}ms artificial delay before taking storage snapshot'
SETTINGS max_rows_to_read = 0;

DROP TABLE mv1;
DROP TABLE mv2;
DROP TABLE src;
DROP TABLE dst1;
DROP TABLE dst2;
"

# `merge` with a `Distributed` sibling analyzes its children again. The child that is the source table
# reads the real table, so it must not get a snapshot without parts. Only the rows inserted before the
# view are counted: the inserted row may be visible or not.
$CLICKHOUSE_CLIENT -nm --query "
DROP TABLE IF EXISTS mv3;
DROP TABLE IF EXISTS src3;
DROP TABLE IF EXISTS other3;
DROP TABLE IF EXISTS dist3;
DROP TABLE IF EXISTS dst3;

CREATE TABLE src3 (id UInt64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE other3 (id UInt64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE dist3 (id UInt64) ENGINE = Distributed(test_shard_localhost, currentDatabase(), other3);
CREATE TABLE dst3 (id UInt64, c_merge_if UInt64, c_merge_where UInt64, c_merge_src_only UInt64) ENGINE = MergeTree ORDER BY id;

INSERT INTO src3 VALUES (100), (101), (102);
INSERT INTO other3 VALUES (200);

CREATE MATERIALIZED VIEW mv3 TO dst3 AS
SELECT id,
    (SELECT countIf(id >= 100) FROM merge(currentDatabase(), '^src3\$|^dist3\$')) AS c_merge_if,
    (SELECT count() FROM merge(currentDatabase(), '^src3\$|^dist3\$') WHERE id >= 100) AS c_merge_where,
    (SELECT countIf(id >= 100) FROM merge(currentDatabase(), '^src3\$')) AS c_merge_src_only
FROM src3;

INSERT INTO src3 VALUES (1);

SELECT 'dst3', * FROM dst3 ORDER BY id;

DROP TABLE mv3;
DROP TABLE src3;
DROP TABLE other3;
DROP TABLE dist3;
DROP TABLE dst3;
"
