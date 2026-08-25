#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The proxy tables of a multi-shard `Remote` database carry an implicit `rand()` sharding key that
# makes them writable by default. The key must only distribute the inserted rows: for reading, the
# proxy must behave like a `Distributed` table without a sharding key, and `SHOW CREATE TABLE` must
# include the key so that the emitted definition recreates a table that is writable as well.
# Both shards point to the same local table here, so a read sees every row twice and an `INSERT`
# adds exactly one row to the local table wherever it goes. The database is created without
# credentials so that the emitted definition carries no password to be masked and can be replayed
# verbatim.

REMOTE_DB="${CLICKHOUSE_DATABASE}_remote_sharded"

${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${CLICKHOUSE_DATABASE}.t (id UInt64, s String) ENGINE = MergeTree ORDER BY id;
    INSERT INTO ${CLICKHOUSE_DATABASE}.t VALUES (1, 'a'), (2, 'b'), (3, 'c');
    CREATE DATABASE ${REMOTE_DB} ENGINE = Remote('127.0.0.1,127.0.0.1', '${CLICKHOUSE_DATABASE}');
"

echo '-- the implicit sharding key is not a shard-pruning key: reads work under force_optimize_skip_unused_shards'
${CLICKHOUSE_CLIENT} --query "
    SET optimize_skip_unused_shards = 1;
    SET force_optimize_skip_unused_shards = 1; -- only for tables with a sharding key
    SELECT count() FROM ${REMOTE_DB}.t;
"

echo '-- the strictest mode keeps rejecting the query, exactly like a Distributed table without a sharding key'
${CLICKHOUSE_CLIENT} --query "
    SET optimize_skip_unused_shards = 1;
    SET force_optimize_skip_unused_shards = 2; -- for all tables
    SELECT count() FROM ${REMOTE_DB}.t;
" 2>&1 | grep -oF 'UNABLE_TO_SKIP_UNUSED_SHARDS' | head -1

echo '-- SHOW CREATE TABLE serializes the implicit sharding key'
${CLICKHOUSE_CLIENT} --query "SHOW CREATE TABLE ${REMOTE_DB}.t FORMAT TSVRaw" | grep -oF 'rand()'

echo '-- the emitted definition recreates a table that accepts a multi-shard INSERT'
RECREATED_DDL=$(${CLICKHOUSE_CLIENT} --query "SHOW CREATE TABLE ${REMOTE_DB}.t FORMAT TSVRaw" \
    | sed "s/CREATE TABLE ${REMOTE_DB}\.t/CREATE TABLE ${CLICKHOUSE_DATABASE}.t_recreated/")
${CLICKHOUSE_CLIENT} --query "${RECREATED_DDL}"
# Unlike the transient proxy, the recreated table inserts through the background spool by default,
# so the insert is pinned to the foreground to make the row visible to the check right away.
${CLICKHOUSE_CLIENT} --query "
    SET distributed_foreground_insert = 1;
    INSERT INTO ${CLICKHOUSE_DATABASE}.t_recreated VALUES (4, 'd');
    SELECT count() FROM ${CLICKHOUSE_DATABASE}.t;
    DROP TABLE ${CLICKHOUSE_DATABASE}.t_recreated;
    DROP DATABASE ${REMOTE_DB};
"
