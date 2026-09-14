#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `DatabaseOverlay` did not override `checkTableNameLength`, so in `clickhouse-local` a long
# `--default_database` left the limit unchecked while the overlay forwarded the create to a real
# on-disk database. The table was accepted and then could not be dropped, because the
# `metadata_dropped` filename derived from the database name exceeds NAME_MAX.

WORKING_FOLDER="${CLICKHOUSE_TMP}/04884_clickhouse_local_overlay_table_name_length"
rm -rf "${WORKING_FOLDER}"
mkdir -p "${WORKING_FOLDER}"

pad() { printf '%*s' "$1" '' | tr ' ' "${2:-d}"; }

# 214 characters saturates the per-table budget to 0, so every table name is rejected.
LONG_DB=$(pad 214)
# 211 leaves a budget of exactly 2: long enough to hold t0, short enough that a 3-character
# destination is refused while this database is the receiver.
RESCUE_DB=$(pad 211)

echo "--- long default_database: CREATE TABLE is rejected up front ---"
${CLICKHOUSE_LOCAL} --path "${WORKING_FOLDER}/long_table" \
    -q "CREATE TABLE tc (a UInt8) ENGINE = MergeTree ORDER BY a" \
    -- --default_database="${LONG_DB}" 2>&1 | grep -o -m1 'ARGUMENT_OUT_OF_BOUND'

echo "--- long default_database: CREATE VIEW is rejected the same way ---"
${CLICKHOUSE_LOCAL} --path "${WORKING_FOLDER}/long_view" \
    -q "CREATE VIEW v AS SELECT 1" \
    -- --default_database="${LONG_DB}" 2>&1 | grep -o -m1 'ARGUMENT_OUT_OF_BOUND'

# RENAME reaches the check through its own call site, so the receiving database needs a nonzero
# budget: the source name must be creatable for the rename to be the statement under test. Each
# statement runs in its own process, because a `-q` list stops at the first error.
echo "--- rescue default_database: the source table is creatable ---"
${CLICKHOUSE_LOCAL} --path "${WORKING_FOLDER}/rename_over" \
    -q "CREATE TABLE t0 (a UInt8) ENGINE = MergeTree ORDER BY a; SELECT 'source created'" \
    -- --default_database="${RESCUE_DB}"
echo "--- rescue default_database: RENAME to a name over the limit is refused ---"
${CLICKHOUSE_LOCAL} --path "${WORKING_FOLDER}/rename_over" \
    -q "RENAME TABLE t0 TO t01" \
    -- --default_database="${RESCUE_DB}" 2>&1 | grep -o -m1 'ARGUMENT_OUT_OF_BOUND'
echo "--- rescue default_database: RENAME within the limit is still accepted ---"
${CLICKHOUSE_LOCAL} --path "${WORKING_FOLDER}/rename_in" \
    -q "CREATE TABLE t0 (a UInt8) ENGINE = MergeTree ORDER BY a" \
    -- --default_database="${RESCUE_DB}"
${CLICKHOUSE_LOCAL} --path "${WORKING_FOLDER}/rename_in" \
    -q "RENAME TABLE t0 TO t9; SELECT 'rename accepted'" \
    -- --default_database="${RESCUE_DB}"

# The limit is read back through the overlay, so this arm follows the disk's real NAME_MAX.
SHORT_DB="db04884"
ALLOWED_LENGTH=$(${CLICKHOUSE_LOCAL} --path "${WORKING_FOLDER}/probe" \
    -q "SELECT getMaxTableNameLengthForDatabase(currentDatabase())" \
    -- --default_database="${SHORT_DB}" | tr -d '[:space:]')

echo "--- short default_database: a name at exactly the limit still works ---"
ALLOWED_NAME=$(pad "${ALLOWED_LENGTH}" t)
${CLICKHOUSE_LOCAL} --path "${WORKING_FOLDER}/boundary" \
    -q "CREATE TABLE ${ALLOWED_NAME} (a UInt8) ENGINE = MergeTree ORDER BY a; DROP TABLE ${ALLOWED_NAME}; SELECT 'boundary ok'" \
    -- --default_database="${SHORT_DB}"

# escapeForFileName emits 3 bytes per non-word byte, so the check must measure the escaped name.
echo "--- short default_database: the escaped length is what counts ---"
ESCAPED_FIT=$(pad $((ALLOWED_LENGTH / 3)) -)
${CLICKHOUSE_LOCAL} --path "${WORKING_FOLDER}/escaped" \
    -q "CREATE TABLE \`${ESCAPED_FIT}\` (a UInt8) ENGINE = MergeTree ORDER BY a; DROP TABLE \`${ESCAPED_FIT}\`; SELECT 'escaped ok'" \
    -- --default_database="${SHORT_DB}"
ESCAPED_OVER=$(pad $((ALLOWED_LENGTH / 3 + 1)) -)
${CLICKHOUSE_LOCAL} --path "${WORKING_FOLDER}/escaped_over" \
    -q "CREATE TABLE \`${ESCAPED_OVER}\` (a UInt8) ENGINE = MergeTree ORDER BY a" \
    -- --default_database="${SHORT_DB}" 2>&1 | grep -o -m1 'ARGUMENT_OUT_OF_BOUND'

echo "--- short default_database: an existing table still loads in a later run ---"
${CLICKHOUSE_LOCAL} --path "${WORKING_FOLDER}/persist" \
    -q "CREATE TABLE t (x UInt64) ENGINE = MergeTree ORDER BY x; INSERT INTO t VALUES (42)" \
    -- --default_database="${SHORT_DB}"
${CLICKHOUSE_LOCAL} --path "${WORKING_FOLDER}/persist" \
    -q "SELECT x FROM t" \
    -- --default_database="${SHORT_DB}"

# ATTACH carries the name past the check by design, and the budget here is 0, so this name is over
# the limit. Reading it back in a later process asserts the load path does not reject it.
echo "--- long default_database: an over-limit name already on disk still loads ---"
${CLICKHOUSE_LOCAL} --path "${WORKING_FOLDER}/attached" \
    -q "ATTACH TABLE ta UUID '11111111-2222-3333-4444-555555555555' (a UInt8) ENGINE = MergeTree ORDER BY a; INSERT INTO ta VALUES (7); SELECT 'attach accepted'" \
    -- --default_database="${LONG_DB}"
${CLICKHOUSE_LOCAL} --path "${WORKING_FOLDER}/attached" \
    -q "SELECT count(), sum(a) FROM ta" \
    -- --default_database="${LONG_DB}"

rm -rf "${WORKING_FOLDER}"
