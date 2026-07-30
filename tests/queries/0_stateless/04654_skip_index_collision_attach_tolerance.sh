#!/usr/bin/env bash
# Tags: no-ordinary-database, no-replicated-database
# no-ordinary-database: ATTACH ... UUID requires an Atomic database.
# no-replicated-database: a Replicated database rewrites the UUID of an ATTACHed table.
# escape_index_filenames defaulted to false before 26.1, so a table created on an older version can
# already hold two skip indices that resolve to the same base stream name. CREATE rejects that pair
# now, but ATTACH must still load such a table: refusing it would turn a data bug into a startup
# failure. The check logs an error instead.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A fresh UUID per run keeps this test safe to run in parallel with itself.
uuid=$(${CLICKHOUSE_CLIENT} -q "SELECT generateUUIDv4()")

definition="(k UInt64, s String, w UInt64,
    INDEX a(s) TYPE text(tokenizer = ngrams(3)) GRANULARITY 1,
    INDEX \`a.dct\` w TYPE set(100) GRANULARITY 1)
ENGINE = MergeTree ORDER BY k SETTINGS escape_index_filenames = 0"

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_attach_collide"

# CREATE is rejected.
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_attach_collide ${definition}" 2>&1 \
    | grep -c -m1 "BAD_ARGUMENTS"

# ATTACH of the very same definition succeeds, and reports the collision as an error in the log.
${CLICKHOUSE_CLIENT} -q "ATTACH TABLE t_attach_collide UUID '${uuid}' ${definition}" 2>&1 \
    | grep -c -m1 "Table definition is incorrect.*collision in file name skp_idx_a.dct"

${CLICKHOUSE_CLIENT} -q "SELECT count() FROM t_attach_collide"
${CLICKHOUSE_CLIENT} -q "DROP TABLE t_attach_collide"

# An inert index type writes no files, so it can never collide with anything and must not be
# reported. ATTACH is the only route that reaches an inert index: its validator rejects a
# non-attaching query outright. A non-inert index in the same shape is reported, which is what makes
# the negative assertion discriminating.
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_inert"

inert_uuid=$(${CLICKHOUSE_CLIENT} -q "SELECT generateUUIDv4()")
${CLICKHOUSE_CLIENT} --send_logs_level=error -q "
    ATTACH TABLE t_inert UUID '${inert_uuid}' (k UInt64, s String, w UInt64,
        INDEX a(s) TYPE text(tokenizer = ngrams(3)) GRANULARITY 1,
        INDEX \`a.dct\` w > 0 TYPE hypothesis GRANULARITY 1)
    ENGINE = MergeTree ORDER BY k SETTINGS escape_index_filenames = 0
" 2>&1 | grep -c "collision in file name"

${CLICKHOUSE_CLIENT} -q "SELECT count() FROM t_inert"
${CLICKHOUSE_CLIENT} -q "DROP TABLE t_inert"

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_not_inert"

not_inert_uuid=$(${CLICKHOUSE_CLIENT} -q "SELECT generateUUIDv4()")
${CLICKHOUSE_CLIENT} --send_logs_level=error -q "
    ATTACH TABLE t_not_inert UUID '${not_inert_uuid}' (k UInt64, s String, w UInt64,
        INDEX a(s) TYPE text(tokenizer = ngrams(3)) GRANULARITY 1,
        INDEX \`a.dct\` w TYPE minmax GRANULARITY 1)
    ENGINE = MergeTree ORDER BY k SETTINGS escape_index_filenames = 0
" 2>&1 | grep -c -m1 "collision in file name"

${CLICKHOUSE_CLIENT} -q "DROP TABLE t_not_inert"
