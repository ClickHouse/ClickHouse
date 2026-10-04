#!/usr/bin/env bash
# A lazily loaded table is a proxy until first access. The row policy of the table an `Alias`
# reads must apply either way, so it must survive the proxy.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

DB="${CLICKHOUSE_DATABASE}_lazy"

${CLICKHOUSE_CLIENT} -q "DROP DATABASE IF EXISTS ${DB} SYNC"
${CLICKHOUSE_CLIENT} -q "CREATE DATABASE ${DB} ENGINE = Atomic SETTINGS lazy_load_tables = 1"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE ${DB}.base (id UInt32, tenant_id UInt32) ENGINE = MergeTree ORDER BY id"
${CLICKHOUSE_CLIENT} -q "INSERT INTO ${DB}.base VALUES (1, 1), (2, 2), (3, 1)"
${CLICKHOUSE_CLIENT} --allow_experimental_alias_table_engine 1 -q "CREATE TABLE ${DB}.al ENGINE = Alias('${DB}', 'base')"
${CLICKHOUSE_CLIENT} -q "CREATE ROW POLICY rp_lazy_${CLICKHOUSE_DATABASE} ON ${DB}.base FOR SELECT USING tenant_id = 1 TO CURRENT_USER"

${CLICKHOUSE_CLIENT} -q "SELECT 'eagerly loaded', arraySort(groupArray(id)) FROM ${DB}.al"

${CLICKHOUSE_CLIENT} -q "DETACH DATABASE ${DB} SYNC"
${CLICKHOUSE_CLIENT} -q "ATTACH DATABASE ${DB}"
# Read the alias before anything else materializes the proxy of its target.
${CLICKHOUSE_CLIENT} -q "SELECT 'lazily loaded', arraySort(groupArray(id)) FROM ${DB}.al"

${CLICKHOUSE_CLIENT} -q "DROP ROW POLICY rp_lazy_${CLICKHOUSE_DATABASE} ON ${DB}.base"
${CLICKHOUSE_CLIENT} -q "DROP DATABASE ${DB} SYNC"
