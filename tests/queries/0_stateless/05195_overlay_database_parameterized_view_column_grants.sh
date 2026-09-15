#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A parameterized view reached through a read-only `Overlay` facade (`ov.v(...)`) keeps the
# column-grant behaviour of a plain parameterized view: a user granted `SELECT` on some columns of
# both the facade name and the underlying source view can read exactly those columns (and run
# trivial queries such as `SELECT count()`), while the other columns stay denied. Resolving the
# view (also for `EXPLAIN QUERY TREE`, which never reaches the planner) requires at least one
# visible column on both names, so a whole-facade grant without any source-side column is denied,
# and the denial names the facade only. The view runs as its definer, so the users below need
# grants on the view names only, exactly as in `05076_additional_table_filters_column_access`.

SUF="${CLICKHOUSE_TEST_UNIQUE_NAME}"

DB_SRC="db_src_${SUF}"
DB_OVL="db_ovl_${SUF}"

USER_COL="u_col_${SUF}"        # SELECT(id) on the facade view and on the source view
USER_SPLIT="u_split_${SUF}"    # SELECT(id) on the facade view, SELECT(s) on the source view
USER_FACADE="u_facade_${SUF}"  # whole-facade SELECT, SHOW TABLES only on the source

${CLICKHOUSE_CLIENT} -m --query "
    DROP DATABASE IF EXISTS ${DB_OVL};
    DROP DATABASE IF EXISTS ${DB_SRC};
    DROP USER IF EXISTS ${USER_COL}, ${USER_SPLIT}, ${USER_FACADE};

    CREATE DATABASE ${DB_SRC};
    CREATE TABLE ${DB_SRC}.t (id UInt32, s String, secret String) ENGINE = MergeTree ORDER BY id;
    INSERT INTO ${DB_SRC}.t VALUES (1, 'a', 'x'), (2, 'b', 'y'), (150, 'big', 'z');
    CREATE VIEW ${DB_SRC}.v SQL SECURITY DEFINER DEFINER = CURRENT_USER AS SELECT id, s, secret FROM ${DB_SRC}.t WHERE id >= {min:UInt32};

    CREATE DATABASE ${DB_OVL} ENGINE = Overlay('${DB_SRC}');

    CREATE USER ${USER_COL} IDENTIFIED WITH no_password;
    CREATE USER ${USER_SPLIT} IDENTIFIED WITH no_password;
    CREATE USER ${USER_FACADE} IDENTIFIED WITH no_password;

    GRANT SELECT(id) ON ${DB_OVL}.v TO ${USER_COL};
    GRANT SELECT(id) ON ${DB_SRC}.v TO ${USER_COL};

    GRANT SELECT(id) ON ${DB_OVL}.v TO ${USER_SPLIT};
    GRANT SELECT(s) ON ${DB_SRC}.v TO ${USER_SPLIT};

    GRANT SELECT ON ${DB_OVL}.* TO ${USER_FACADE};
    GRANT SHOW TABLES ON ${DB_SRC}.* TO ${USER_FACADE};
"

# Prints the query result or the error code. A column-level denial for a user who already holds a
# grant on the source view may name the source (the planner reports the missing columns per name,
# as it would for a direct read of the source); a resolution-time denial for a user without any
# source-side column must not, so `run_hidden` reports whether the source database was named.
function run_impl
{
    local report_naming="$1"
    local user="$2"
    local query="$3"
    local out
    out=$(${CLICKHOUSE_CLIENT} --user "${user}" --enable_analyzer 1 --query "${query}" 2>&1)
    if echo "${out}" | grep -q 'ACCESS_DENIED'; then
        if [ "${report_naming}" = 1 ] && echo "${out}" | grep -q "${DB_SRC}"; then
            echo "ACCESS_DENIED naming the source"
        else
            echo "ACCESS_DENIED"
        fi
    elif echo "${out}" | grep -q 'UNKNOWN_FUNCTION'; then
        echo "UNKNOWN_FUNCTION"
    else
        echo "${out}"
    fi
}

function run
{
    run_impl 0 "$1" "$2"
}

function run_hidden
{
    run_impl 1 "$1" "$2"
}

# `EXPLAIN QUERY TREE` output is long: report only whether the view resolved.
function explain
{
    local user="$1"
    local query="$2"
    local out
    out=$(run_hidden "${user}" "EXPLAIN QUERY TREE ${query}")
    if echo "${out}" | grep -q 'ACCESS_DENIED\|UNKNOWN_FUNCTION\|UNKNOWN_TABLE'; then
        echo "${out}"
    elif echo "${out}" | grep -q 'TABLE_FUNCTION\|TABLE id'; then
        echo "resolved"
    else
        echo "unexpected output"
    fi
}

echo "column-granted user on both names: trivial queries and the granted column work"
run "${USER_COL}" "SELECT count() FROM ${DB_OVL}.v(min = 0)"
run "${USER_COL}" "SELECT id FROM ${DB_OVL}.v(min = 2) ORDER BY id"
explain "${USER_COL}" "SELECT id FROM ${DB_OVL}.v(min = 0)"

echo "column-granted user on both names: the other columns stay denied"
run "${USER_COL}" "SELECT s FROM ${DB_OVL}.v(min = 0)"
run "${USER_COL}" "SELECT * FROM ${DB_OVL}.v(min = 0)"

echo "different columns granted on the two names: no column is readable through the facade"
run "${USER_SPLIT}" "SELECT count() FROM ${DB_OVL}.v(min = 0)"
run "${USER_SPLIT}" "SELECT id FROM ${DB_OVL}.v(min = 0)"
run "${USER_SPLIT}" "SELECT s FROM ${DB_OVL}.v(min = 0)"

echo "whole-facade grant without a source-side column: denied, the source is not named"
run_hidden "${USER_FACADE}" "SELECT count() FROM ${DB_OVL}.v(min = 0)"
explain "${USER_FACADE}" "SELECT id FROM ${DB_OVL}.v(min = 0)"
explain "${USER_FACADE}" "SELECT secret FROM ${DB_OVL}.t"

echo "after granting a source-side column, the facade grant is no longer the limit"
${CLICKHOUSE_CLIENT} --query "GRANT SELECT(id) ON ${DB_SRC}.v TO ${USER_FACADE}"
run "${USER_FACADE}" "SELECT count() FROM ${DB_OVL}.v(min = 0)"
run "${USER_FACADE}" "SELECT id FROM ${DB_OVL}.v(min = 2) ORDER BY id"
run "${USER_FACADE}" "SELECT secret FROM ${DB_OVL}.v(min = 0)"
explain "${USER_FACADE}" "SELECT id FROM ${DB_OVL}.v(min = 0)"

${CLICKHOUSE_CLIENT} -m --query "
    DROP USER ${USER_COL}, ${USER_SPLIT}, ${USER_FACADE};
    DROP DATABASE ${DB_OVL};
    DROP DATABASE ${DB_SRC};
"
