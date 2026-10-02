#!/usr/bin/env bash

# The local shard of a `remote` or `Distributed` read inside a materialized view query
# reads the source table itself, not the inserted block, like the remote shards do.
# The database name is spelled out: `currentDatabase()` in the view query does not resolve
# to the test database while the block is pushed to the view.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -m -q "
DROP TABLE IF EXISTS mv;
DROP TABLE IF EXISTS mv_dist;
DROP TABLE IF EXISTS dst;
DROP TABLE IF EXISTS dist;
DROP TABLE IF EXISTS src;

CREATE TABLE src (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO src SELECT number + 100 FROM numbers(10);

CREATE TABLE dist AS src ENGINE = Distributed(test_shard_localhost, ${CLICKHOUSE_DATABASE}, src);

CREATE TABLE dst (name String, block_rows UInt64, table_rows UInt64) ENGINE = MergeTree ORDER BY name;

CREATE MATERIALIZED VIEW mv TO dst AS
    SELECT 'remote' AS name, count() AS block_rows,
        (SELECT count() FROM remote('127.0.0.1', ${CLICKHOUSE_DATABASE}, src) WHERE x >= 100) AS table_rows
    FROM src;

CREATE MATERIALIZED VIEW mv_dist TO dst AS
    SELECT 'distributed' AS name, count() AS block_rows,
        (SELECT count() FROM ${CLICKHOUSE_DATABASE}.dist WHERE x >= 100) AS table_rows
    FROM src;

INSERT INTO src VALUES (1), (2), (3);

SELECT * FROM dst ORDER BY name;

DROP TABLE mv;
DROP TABLE mv_dist;
DROP TABLE dst;
DROP TABLE dist;
DROP TABLE src;
"
