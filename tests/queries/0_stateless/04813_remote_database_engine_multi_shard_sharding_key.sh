#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The proxy tables of a multi-shard `Remote` database carry an implicit `rand()` sharding key that
# makes them writable by default. The key must only distribute the inserted rows: for reading, the
# proxy must behave like a `Distributed` table without a sharding key. No standalone `Remote` table
# definition can preserve that insert-only behavior, so `SHOW CREATE TABLE` must reject it.
# Both shards point to the same local table here, so a read sees every row twice and an `INSERT`
# adds exactly one row to the local table wherever it goes. The database is created without
# credentials because the test exercises only the proxy behavior.

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

echo '-- SHOW CREATE TABLE rejects a misleading standalone Remote definition'
${CLICKHOUSE_CLIENT} --query "SHOW CREATE TABLE ${REMOTE_DB}.t FORMAT TSVRaw" 2>&1 \
    | grep -oF 'THERE_IS_NO_QUERY' | head -1

${CLICKHOUSE_CLIENT} --query "DROP DATABASE ${REMOTE_DB};"
