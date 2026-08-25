#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `SHOW CREATE TABLE` for a non-existent table must suggest a similarly-named table,
# just like `SELECT` does. Previously it threw the bare `UNKNOWN_TABLE` /
# `CANNOT_GET_CREATE_TABLE_QUERY` error with no "Maybe you meant ...?" hint.
#
# The test database name is dynamic, so it is normalized to `{db}` in the output.
DB="${CLICKHOUSE_DATABASE}"

# Atomic database (the default): table metadata lives on disk, so a missing table is
# reported with `CANNOT_GET_CREATE_TABLE_QUERY`.
${CLICKHOUSE_CLIENT} -q "CREATE TABLE ${DB}.some_target_table (x UInt8) ENGINE = MergeTree ORDER BY x"

# grep -m1: with --send_logs_level the server can also echo the exception as a log event,
# so the hint may appear more than once; take a single match to stay deterministic.
${CLICKHOUSE_CLIENT} -q "SHOW CREATE TABLE ${DB}.some_target_tabl" 2>&1 \
    | grep -oF -m1 "Maybe you meant ${DB}.some_target_table?" | sed "s/${DB}/{db}/g"

# The unqualified form (resolved against the current database, which is ${CLICKHOUSE_DATABASE})
# behaves the same.
${CLICKHOUSE_CLIENT} -q "SHOW CREATE TABLE some_target_tabl" 2>&1 \
    | grep -oF -m1 "Maybe you meant ${DB}.some_target_table?" | sed "s/${DB}/{db}/g"

# `SHOW CREATE VIEW` and `SHOW CREATE DICTIONARY` go through the same code path.
${CLICKHOUSE_CLIENT} -q "SHOW CREATE VIEW ${DB}.some_target_tabl" 2>&1 \
    | grep -oF -m1 "Maybe you meant ${DB}.some_target_table?" | sed "s/${DB}/{db}/g"

# Memory-engine database: a missing table is reported with `UNKNOWN_TABLE` (a different
# error code), which must carry the hint as well.
${CLICKHOUSE_CLIENT} -q "CREATE DATABASE ${DB}_mem ENGINE = Memory"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE ${DB}_mem.mem_target (x UInt8) ENGINE = Memory"
${CLICKHOUSE_CLIENT} -q "SHOW CREATE TABLE ${DB}_mem.mem_targe" 2>&1 \
    | grep -oF -m1 "Maybe you meant ${DB}_mem.mem_target?" | sed "s/${DB}/{db}/g"
${CLICKHOUSE_CLIENT} -q "DROP DATABASE ${DB}_mem"

# `Dictionary`-engine database: it exposes every loaded dictionary as a virtual table, and
# reports a missing one with `CANNOT_GET_CREATE_DICTIONARY_QUERY` (yet another error code),
# which must carry the hint too. The dictionary's owning database has to be `Ordinary` (not the
# default `Atomic`) so the virtual table is named `<source_database>.<dictionary_name>` - with
# `Atomic` it is named after the dictionary's UUID instead, which is not suitable for this test.
# `--send_logs_level=fatal` keeps the `Ordinary` engine deprecation warning (emitted once per
# server lifetime, on the first CREATE of an `Ordinary` database) out of the test's stderr.
${CLICKHOUSE_CLIENT} --send_logs_level=fatal --allow_deprecated_database_ordinary=1 -q "CREATE DATABASE ${DB}_ord ENGINE = Ordinary"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE ${DB}_ord.dict_source (key UInt64, val UInt64) ENGINE = Memory"
${CLICKHOUSE_CLIENT} -q "CREATE DICTIONARY ${DB}_ord.dict_target (key UInt64 DEFAULT 0, val UInt64 DEFAULT 0) PRIMARY KEY key SOURCE(CLICKHOUSE(TABLE 'dict_source' DB '${DB}_ord')) LIFETIME(MIN 0 MAX 0) LAYOUT(FLAT())"
${CLICKHOUSE_CLIENT} -q "CREATE DATABASE ${DB}_dictdb ENGINE = Dictionary"
${CLICKHOUSE_CLIENT} -q "SHOW CREATE TABLE ${DB}_dictdb.\`${DB}_ord.dict_targe\`" 2>&1 \
    | grep -oF -m1 "Maybe you meant ${DB}_dictdb.\`${DB}_ord.dict_target\`?" | sed "s/${DB}/{db}/g"
${CLICKHOUSE_CLIENT} -q "DROP DATABASE ${DB}_dictdb"
${CLICKHOUSE_CLIENT} -q "DROP DATABASE ${DB}_ord"

# When no similarly-named table exists, there must be no hint (0 matching lines).
${CLICKHOUSE_CLIENT} -q "SHOW CREATE TABLE ${DB}.nothing_is_close_to_this_xyz" 2>&1 \
    | grep -c -F "Maybe you meant" || true
