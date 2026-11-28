#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `SHOW CREATE` checks access on the *requested* identifier before any lookup or hinting. The
# "Maybe you meant ...?" hint therefore only appears for grants that cover the requested name -
# a broad `db.*` / `*.*` grant, or the exact name itself (see 04545/04546). With an *object-level*
# `SHOW` grant scoped to one specific name, a misspelled name is not covered, so the access check
# denies it first and no hint is produced.
#
# This is deliberate: unlike `SELECT` (whose analyzer resolves the name before the access check and
# so leaks whether an unqualified name exists), `SHOW CREATE` must not become an existence oracle.
# With an object-level grant an existing-but-hidden object and a missing name are indistinguishable:
# both are reported as `ACCESS_DENIED`, so a user cannot tell them apart for a name they are not
# granted on.
#
# The test database name is dynamic, so it is normalized to `{db}` in the output.
DB="${CLICKHOUSE_DATABASE}"
U_DICT="user_dict_${CLICKHOUSE_TEST_UNIQUE_NAME}"
U_COL="user_col_${CLICKHOUSE_TEST_UNIQUE_NAME}"
U_BROAD="user_broad_${CLICKHOUSE_TEST_UNIQUE_NAME}"

${CLICKHOUSE_CLIENT} -q "CREATE TABLE ${DB}.src (key UInt64, val UInt64) ENGINE = Memory"
${CLICKHOUSE_CLIENT} -q "CREATE DICTIONARY ${DB}.dict_target (key UInt64 DEFAULT 0, val UInt64 DEFAULT 0) PRIMARY KEY key SOURCE(CLICKHOUSE(TABLE 'src' DB '${DB}')) LIFETIME(MIN 0 MAX 0) LAYOUT(FLAT())"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE ${DB}.tbl_target (x UInt8) ENGINE = Memory"
${CLICKHOUSE_CLIENT} -q "CREATE VIEW ${DB}.view_target AS SELECT 1 AS x"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE ${DB}.hidden_table (x UInt8) ENGINE = Memory"

# --- Object-level SHOW DICTIONARIES grant (dictionary surface) ---
${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${U_DICT}"
${CLICKHOUSE_CLIENT} -q "CREATE USER ${U_DICT}"
${CLICKHOUSE_CLIENT} -q "GRANT SHOW DICTIONARIES ON ${DB}.dict_target TO ${U_DICT}"

# The exact granted name still works.
${CLICKHOUSE_CLIENT} --user "${U_DICT}" -q "SHOW CREATE DICTIONARY ${DB}.dict_target" 2>&1 \
    | grep -oF -m1 "CREATE DICTIONARY ${DB}.dict_target" | sed "s/${DB}/{db}/g"

# A misspelled name is denied by the access check first - no hint. (grep -m1 -c: the exception may
# also be echoed as a server log with --send_logs_level, so cap the count at a single match.)
${CLICKHOUSE_CLIENT} --user "${U_DICT}" -q "SHOW CREATE DICTIONARY ${DB}.dict_targe" 2>&1 \
    | grep -m1 -c -F "ACCESS_DENIED" || true
${CLICKHOUSE_CLIENT} --user "${U_DICT}" -q "SHOW CREATE DICTIONARY ${DB}.dict_targe" 2>&1 \
    | grep -c -F "Maybe you meant" || true

# Oracle safety: an existing-but-hidden object is reported identically to a missing name. Here
# `hidden_table` exists but the user has no grant on it, so it is denied - just like `dict_targe`.
${CLICKHOUSE_CLIENT} --user "${U_DICT}" -q "SHOW CREATE DICTIONARY ${DB}.hidden_table" 2>&1 \
    | grep -m1 -c -F "ACCESS_DENIED" || true

# --- Object-level SHOW COLUMNS grant (table / view surface) ---
${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${U_COL}"
${CLICKHOUSE_CLIENT} -q "CREATE USER ${U_COL}"
${CLICKHOUSE_CLIENT} -q "GRANT SHOW COLUMNS ON ${DB}.tbl_target TO ${U_COL}"
${CLICKHOUSE_CLIENT} -q "GRANT SHOW COLUMNS ON ${DB}.view_target TO ${U_COL}"

# The exact granted name still works.
${CLICKHOUSE_CLIENT} --user "${U_COL}" -q "SHOW CREATE TABLE ${DB}.tbl_target" 2>&1 \
    | grep -oF -m1 "CREATE TABLE ${DB}.tbl_target" | sed "s/${DB}/{db}/g"

# A misspelled table name is denied first - no hint, even though `tbl_target` is one edit away.
${CLICKHOUSE_CLIENT} --user "${U_COL}" -q "SHOW CREATE TABLE ${DB}.tbl_targe" 2>&1 \
    | grep -m1 -c -F "ACCESS_DENIED" || true
${CLICKHOUSE_CLIENT} --user "${U_COL}" -q "SHOW CREATE TABLE ${DB}.tbl_targe" 2>&1 \
    | grep -c -F "Maybe you meant" || true

# The same holds for `SHOW CREATE VIEW`, which is also authorized with `SHOW COLUMNS`.
${CLICKHOUSE_CLIENT} --user "${U_COL}" -q "SHOW CREATE VIEW ${DB}.view_targe" 2>&1 \
    | grep -m1 -c -F "ACCESS_DENIED" || true
${CLICKHOUSE_CLIENT} --user "${U_COL}" -q "SHOW CREATE VIEW ${DB}.view_targe" 2>&1 \
    | grep -c -F "Maybe you meant" || true

# --- Contrast: a broader grant that *does* cover the requested name keeps the hint ---
${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${U_BROAD}"
${CLICKHOUSE_CLIENT} -q "CREATE USER ${U_BROAD}"
${CLICKHOUSE_CLIENT} -q "GRANT SHOW DICTIONARIES ON ${DB}.* TO ${U_BROAD}"
${CLICKHOUSE_CLIENT} --user "${U_BROAD}" -q "SHOW CREATE DICTIONARY ${DB}.dict_targe" 2>&1 \
    | grep -oF -m1 "Maybe you meant ${DB}.dict_target?" | sed "s/${DB}/{db}/g"

${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${U_DICT}"
${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${U_COL}"
${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${U_BROAD}"
