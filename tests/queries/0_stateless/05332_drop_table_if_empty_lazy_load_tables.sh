#!/usr/bin/env bash
# Tags: no-replicated-database
# `DROP TABLE IF EMPTY` in a `lazy_load_tables = 1` database: an unloaded table is a
# `StorageTableProxy` that reports no row count, so `IF EMPTY` must load it before judging it,
# otherwise an empty table is refused as if it were not empty.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

LAZY="${CLICKHOUSE_DATABASE}_lazy"

${CLICKHOUSE_CLIENT} -q "DROP DATABASE IF EXISTS \`${LAZY}\` SYNC"
${CLICKHOUSE_CLIENT} -m -q "
    CREATE DATABASE \`${LAZY}\` ENGINE = Atomic SETTINGS lazy_load_tables = 1;
    CREATE TABLE \`${LAZY}\`.empty (id UInt64) ENGINE = MergeTree ORDER BY id;
    CREATE TABLE \`${LAZY}\`.full (id UInt64) ENGINE = MergeTree ORDER BY id;
    INSERT INTO \`${LAZY}\`.full SELECT number FROM numbers(10);
"

# Re-attach, so that both tables are unloaded stand-ins again.
${CLICKHOUSE_CLIENT} -q "DETACH DATABASE \`${LAZY}\` SYNC"
${CLICKHOUSE_CLIENT} -q "ATTACH DATABASE \`${LAZY}\`"
${CLICKHOUSE_CLIENT} -q "SELECT name, engine FROM system.tables WHERE database = '${LAZY}' ORDER BY name"

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EMPTY \`${LAZY}\`.empty SYNC"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EMPTY \`${LAZY}\`.full SYNC" 2>&1 | grep -o -m1 "TABLE_NOT_EMPTY"
${CLICKHOUSE_CLIENT} -q "SELECT name FROM system.tables WHERE database = '${LAZY}' ORDER BY name"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM \`${LAZY}\`.full"

${CLICKHOUSE_CLIENT} -q "DROP DATABASE \`${LAZY}\` SYNC"
