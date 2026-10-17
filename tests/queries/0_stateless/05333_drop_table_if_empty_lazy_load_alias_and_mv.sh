#!/usr/bin/env bash
# Tags: no-replicated-database
# `IF EMPTY` in a `lazy_load_tables = 1` database for storages that report the row count of their
# target: an `Alias` (for `TRUNCATE`) and a materialized view with an inner table. Their targets
# are unloaded stand-ins after the database is re-attached, and must be loaded before judging, so
# that an empty target is not refused as if it were not empty.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

LAZY="${CLICKHOUSE_DATABASE}_lazy"

${CLICKHOUSE_CLIENT} -q "DROP DATABASE IF EXISTS \`${LAZY}\` SYNC"
${CLICKHOUSE_CLIENT} -m -q "
    CREATE DATABASE \`${LAZY}\` ENGINE = Atomic SETTINGS lazy_load_tables = 1;
    CREATE TABLE \`${LAZY}\`.src (id UInt64) ENGINE = MergeTree ORDER BY id;
    CREATE TABLE \`${LAZY}\`.empty (id UInt64) ENGINE = MergeTree ORDER BY id;
    CREATE TABLE \`${LAZY}\`.full (id UInt64) ENGINE = MergeTree ORDER BY id;
    INSERT INTO \`${LAZY}\`.full SELECT number FROM numbers(10);
    CREATE TABLE \`${LAZY}\`.alias_empty ENGINE = Alias('${LAZY}', 'empty');
    CREATE TABLE \`${LAZY}\`.alias_full ENGINE = Alias('${LAZY}', 'full');
    CREATE MATERIALIZED VIEW \`${LAZY}\`.mv_empty ENGINE = MergeTree ORDER BY id AS SELECT id FROM \`${LAZY}\`.src;
"

# Re-attach, so that all tables are unloaded stand-ins again.
${CLICKHOUSE_CLIENT} -q "DETACH DATABASE \`${LAZY}\` SYNC"
${CLICKHOUSE_CLIENT} -q "ATTACH DATABASE \`${LAZY}\`"

${CLICKHOUSE_CLIENT} -q "TRUNCATE TABLE IF EMPTY \`${LAZY}\`.alias_empty"
${CLICKHOUSE_CLIENT} -q "TRUNCATE TABLE IF EMPTY \`${LAZY}\`.alias_full" 2>&1 | grep -o -m1 "TABLE_NOT_EMPTY"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EMPTY \`${LAZY}\`.mv_empty SYNC"
${CLICKHOUSE_CLIENT} -q "SELECT name FROM system.tables WHERE database = '${LAZY}' AND name NOT LIKE '.inner%' ORDER BY name"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM \`${LAZY}\`.full"

${CLICKHOUSE_CLIENT} -q "DROP DATABASE \`${LAZY}\` SYNC"
