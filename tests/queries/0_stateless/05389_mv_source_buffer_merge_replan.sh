#!/usr/bin/env bash

# Inside a materialized view query, a `Buffer` over a `Distributed` table plans the query again for its buffered rows,
# and a `merge` table with a `Distributed` child plans it again for the source table. The source table read by that
# plan is the table, while the IN subqueries of the view query, also with a table on the right, check those rows
# against the inserted block. Rows already in the `Distributed` table are read on the shard, where the source table is
# the table, so they are checked against the table. A refreshable view appending to the source table behaves like an
# INSERT. The values do not depend on whether the inserted rows are already in the table. The database name is spelled
# out: `currentDatabase()` in the view query does not resolve to the test database during the push.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -m -q "
DROP DATABASE IF EXISTS ${CLICKHOUSE_DATABASE_1};
DROP TABLE IF EXISTS mv_merge_not_in;
DROP TABLE IF EXISTS mv_merge_not_in_table;
DROP TABLE IF EXISTS mv_buffer_not_in_table;
DROP TABLE IF EXISTS mv_buffer_join;
DROP TABLE IF EXISTS dst;
DROP TABLE IF EXISTS buf;
DROP TABLE IF EXISTS dist;
DROP TABLE IF EXISTS dist_target;
DROP TABLE IF EXISTS src;
DROP TABLE IF EXISTS src_dup;

CREATE TABLE src (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO src SELECT number + 100 FROM numbers(10);
CREATE TABLE src_dup (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO src_dup SELECT number + 100 FROM numbers(10);

CREATE TABLE dist_target (x UInt64) ENGINE = MergeTree ORDER BY x;
CREATE TABLE dist AS dist_target ENGINE = Distributed(test_shard_localhost, ${CLICKHOUSE_DATABASE}, dist_target);
CREATE TABLE buf AS dist_target ENGINE = Buffer(${CLICKHOUSE_DATABASE}, dist, 1, 100000, 100000, 100000, 100000, 100000000, 100000000);
INSERT INTO buf SELECT number + 100 FROM numbers(10);
-- Rows already flushed from 'buf': 100 is in the source tables and 110 is not, so the table keeps 1 of them, the block 2.
INSERT INTO dist_target VALUES (100), (110);

CREATE TABLE dst (name String, block_rows UInt64, value UInt64) ENGINE = MergeTree ORDER BY name;

-- 'dist' raises the merge read above FetchColumns, so 'src' is planned again. The NOT IN subquery maps the inserted
-- rows onto three rows of the table: the block keeps 7 rows of 'src', the table 0; 'dist' adds 1.
CREATE MATERIALIZED VIEW mv_merge_not_in TO dst AS
    SELECT 'merge_not_in' AS name, count() AS block_rows,
        (SELECT count() FROM merge(${CLICKHOUSE_DATABASE}, '^(src|dist)\$')
         WHERE x >= 100 AND x NOT IN (SELECT n.number + 100 * (s.x < 100) FROM numbers(200) AS n JOIN src AS s ON n.number = s.x)) AS value
    FROM src;

-- 'src_dup' gets three rows it already has: the block keeps 7 rows of 'src_dup' and of 'buf', the table 0; 'dist' adds 1.
CREATE MATERIALIZED VIEW mv_merge_not_in_table TO dst AS
    SELECT 'merge_not_in_table' AS name, count() AS block_rows,
        (SELECT count() FROM merge(${CLICKHOUSE_DATABASE}, '^(src_dup|dist)\$') WHERE x >= 100 AND x NOT IN src_dup) AS value
    FROM src_dup;

CREATE MATERIALIZED VIEW mv_buffer_not_in_table TO dst AS
    SELECT 'buffer_not_in_table' AS name, count() AS block_rows,
        (SELECT count() FROM buf WHERE x NOT IN src_dup) AS value
    FROM src_dup;

-- The JOIN is sent to the shard of 'dist', where 'src' is the table, as for a JOIN over 'dist' itself, and the buffered
-- rows are planned again with the same query: 9 buffered rows and 1 flushed row match the table, none the block.
CREATE MATERIALIZED VIEW mv_buffer_join TO dst AS
    SELECT 'buffer_join' AS name, count() AS block_rows,
        (SELECT count() FROM buf AS b JOIN src AS s ON b.x = s.x + 1) AS value
    FROM src;

INSERT INTO src VALUES (1), (2), (3);
INSERT INTO src_dup VALUES (101), (102), (103);

SELECT 'insert', * FROM dst ORDER BY name;

-- The refreshable views live in an Atomic database: in a Replicated one an APPEND refresh runs on one replica only.
TRUNCATE TABLE dst;
CREATE DATABASE ${CLICKHOUSE_DATABASE_1} ENGINE = Atomic;
CREATE MATERIALIZED VIEW ${CLICKHOUSE_DATABASE_1}.rmv_append REFRESH EVERY 1 YEAR APPEND TO ${CLICKHOUSE_DATABASE}.src EMPTY AS
    SELECT number + 4 AS x FROM numbers(3);
CREATE MATERIALIZED VIEW ${CLICKHOUSE_DATABASE_1}.rmv_append_dup REFRESH EVERY 1 YEAR APPEND TO ${CLICKHOUSE_DATABASE}.src_dup EMPTY AS
    SELECT number + 101 AS x FROM numbers(3);
SYSTEM REFRESH VIEW ${CLICKHOUSE_DATABASE_1}.rmv_append;
SYSTEM WAIT VIEW ${CLICKHOUSE_DATABASE_1}.rmv_append;
SYSTEM REFRESH VIEW ${CLICKHOUSE_DATABASE_1}.rmv_append_dup;
SYSTEM WAIT VIEW ${CLICKHOUSE_DATABASE_1}.rmv_append_dup;

SELECT 'append_refresh', * FROM dst ORDER BY name;

-- A refreshable view reading the source table has no inserted block, so it reads the table.
TRUNCATE TABLE dst;
CREATE MATERIALIZED VIEW ${CLICKHOUSE_DATABASE_1}.rmv_read REFRESH EVERY 1 YEAR APPEND TO ${CLICKHOUSE_DATABASE}.dst EMPTY AS
    SELECT 'buffer_in' AS name, count() AS block_rows,
        (SELECT count() FROM buf WHERE x IN (SELECT x FROM src_dup WHERE x < 105)) AS value
    FROM ${CLICKHOUSE_DATABASE}.src_dup
    UNION ALL
    SELECT 'merge_in', count(),
        (SELECT count() FROM merge(${CLICKHOUSE_DATABASE}, '^(src_dup|dist)\$') WHERE x >= 100 AND x IN (SELECT x FROM src_dup WHERE x < 105))
    FROM ${CLICKHOUSE_DATABASE}.src_dup;
SYSTEM REFRESH VIEW ${CLICKHOUSE_DATABASE_1}.rmv_read;
SYSTEM WAIT VIEW ${CLICKHOUSE_DATABASE_1}.rmv_read;

SELECT 'refreshable', * FROM dst ORDER BY name;

DROP DATABASE ${CLICKHOUSE_DATABASE_1};
DROP TABLE mv_merge_not_in;
DROP TABLE mv_merge_not_in_table;
DROP TABLE mv_buffer_not_in_table;
DROP TABLE mv_buffer_join;
DROP TABLE dst;
DROP TABLE buf;
DROP TABLE dist;
DROP TABLE dist_target;
DROP TABLE src;
DROP TABLE src_dup;
"
