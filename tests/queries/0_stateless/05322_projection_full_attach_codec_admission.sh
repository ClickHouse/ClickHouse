#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
set -euo pipefail

source_table="${CLICKHOUSE_DATABASE}.t_05322_attach_source"
attached_table="${CLICKHOUSE_DATABASE}.t_05322_attach_target"
rejected_uuid=$(${CLICKHOUSE_CLIENT} -q 'SELECT generateUUIDv4()')
accepted_uuid=$(${CLICKHOUSE_CLIENT} -q 'SELECT generateUUIDv4()')

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS ${attached_table} SYNC"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS ${source_table} SYNC"
${CLICKHOUSE_CLIENT} --enable_alp_codec=1 -q "
    CREATE TABLE ${source_table}
    (k UInt64, x Float64, PROJECTION p (x CODEC(ALP)) AS (SELECT k, x ORDER BY k))
    ENGINE = MergeTree ORDER BY k"

rejected_query="ATTACH TABLE ${attached_table} UUID '${rejected_uuid}'
    ENGINE = MergeTree ORDER BY k AS ${source_table}"
if error=$(${CLICKHOUSE_CLIENT} --enable_alp_codec=0 -q "$rejected_query" 2>&1); then
    echo "Full-definition ATTACH unexpectedly accepted ALP without the opt-in" >&2
    exit 1
fi
if [[ "$error" != *BAD_ARGUMENTS* || "$error" != *enable_alp_codec* ]]; then
    echo "Full-definition ATTACH failed for an unexpected reason: $error" >&2
    exit 1
fi
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.tables
    WHERE database = currentDatabase() AND name = 't_05322_attach_target'"

${CLICKHOUSE_CLIENT} --enable_alp_codec=1 -q "ATTACH TABLE ${attached_table} UUID '${accepted_uuid}'
    ENGINE = MergeTree ORDER BY k AS ${source_table}"
${CLICKHOUSE_CLIENT} --enable_alp_codec=0 -q "INSERT INTO ${attached_table} SELECT 1, 3.5"
${CLICKHOUSE_CLIENT} -q "SELECT x FROM ${attached_table} ORDER BY k"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.projections
    WHERE database = currentDatabase() AND table = 't_05322_attach_target' AND name = 'p'"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.projection_parts
    WHERE database = currentDatabase() AND table = 't_05322_attach_target' AND name = 'p' AND active"

${CLICKHOUSE_CLIENT} -q "DROP TABLE ${attached_table} SYNC"
${CLICKHOUSE_CLIENT} -q "DROP TABLE ${source_table} SYNC"
