#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database, no-shared-catalog, no-parallel
# no-fasttest, no-replicated-database, no-shared-catalog -- edits on-disk table metadata and re-reads it via ATTACH DATABASE
# no-parallel -- DETACH/ATTACH DATABASE of a sub-database on the shared server

# Regression test: loading a view whose stored metadata has no column list (as can be
# produced by the AST fuzzer or manual intervention) must not abort the whole server with
# LOGICAL_ERROR "Invalid storage definition in metadata file". A view legitimately has no
# storage engine, so the load must fail with a normal catchable exception and keep the
# server running.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

db="db_04603_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} -q "DROP DATABASE IF EXISTS ${db}"
${CLICKHOUSE_CLIENT} -q "CREATE DATABASE ${db} ENGINE = Atomic"
${CLICKHOUSE_CLIENT} -q "CREATE VIEW ${db}.v (\`x\` UInt8) AS SELECT 1 AS x"

rel_path=$(${CLICKHOUSE_CLIENT} -q "SELECT metadata_path FROM system.tables WHERE database = '${db}' AND name = 'v'")
data_path=$(${CLICKHOUSE_CLIENT} -q "SELECT value FROM system.server_settings WHERE name = 'path'")
meta="${data_path%/}/${rel_path}"

${CLICKHOUSE_CLIENT} -q "DETACH DATABASE ${db}"

# Strip the column list from the on-disk metadata, leaving a columnless view definition.
perl -0777 -pi -e 's/\n\(\n.*?\n\)\n(AS SELECT)/\n$1/s' "$meta"

# Re-reading the metadata must fail gracefully, not abort the server.
${CLICKHOUSE_CLIENT} -q "ATTACH DATABASE ${db}" 2>&1 | grep -o "EMPTY_LIST_OF_COLUMNS_PASSED" | head -n1

# The server must still be alive.
${CLICKHOUSE_CLIENT} -q "SELECT 'server alive'"

${CLICKHOUSE_CLIENT} -q "DROP DATABASE IF EXISTS ${db}"
