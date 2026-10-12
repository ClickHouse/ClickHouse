#!/usr/bin/env bash
# Tags: no-replicated-database
# `IF EMPTY` in a `lazy_load_tables = 1` database for a `TimeSeries` table, which reports the sum of
# the row counts of its inner tables. The inner tables are unloaded stand-ins after the database is
# re-attached, and must be loaded before judging, so that an empty table is not refused as if it
# were not empty.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

LAZY="${CLICKHOUSE_DATABASE}_lazy"

${CLICKHOUSE_CLIENT} -q "DROP DATABASE IF EXISTS \`${LAZY}\` SYNC"
${CLICKHOUSE_CLIENT} -m --allow_experimental_time_series_table 1 -q "
    CREATE DATABASE \`${LAZY}\` ENGINE = Atomic SETTINGS lazy_load_tables = 1;
    CREATE TABLE \`${LAZY}\`.ts_empty ENGINE = TimeSeries;
    CREATE TABLE \`${LAZY}\`.ts_full ENGINE = TimeSeries;
    INSERT INTO \`${LAZY}\`.ts_full (metric_name, tags, samples) VALUES ('metric', map(), [(now64(3), 1.0)]);
"

# Re-attach, so that all tables are unloaded stand-ins again.
${CLICKHOUSE_CLIENT} -q "DETACH DATABASE \`${LAZY}\` SYNC"
${CLICKHOUSE_CLIENT} -q "ATTACH DATABASE \`${LAZY}\`"

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EMPTY \`${LAZY}\`.ts_empty SYNC"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EMPTY \`${LAZY}\`.ts_full SYNC" 2>&1 | grep -o -m1 "TABLE_NOT_EMPTY"
${CLICKHOUSE_CLIENT} -q "SELECT name FROM system.tables WHERE database = '${LAZY}' AND name NOT LIKE '.inner%' ORDER BY name"

${CLICKHOUSE_CLIENT} -q "DROP DATABASE \`${LAZY}\` SYNC"
