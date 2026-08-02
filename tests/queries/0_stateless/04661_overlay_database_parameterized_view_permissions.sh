#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A parameterized view reached through a read-only `Overlay` facade (`ov.v(...)`) follows the same
# dual-grant contract as a plain table: reading requires `SELECT` on both the facade name and the
# underlying source view, `DESCRIBE` requires `SHOW COLUMNS` on both, a user without the
# source-side grant cannot distinguish the view from a missing one (`UNKNOWN_FUNCTION`, not
# `ACCESS_DENIED`), and the row policies of both names are combined.

SUF="${CLICKHOUSE_TEST_UNIQUE_NAME}"

DB_SRC="db_src_${SUF}"
DB_OVL="db_ovl_${SUF}"

USER_OVL="u_ovl_${SUF}"    # grants on the facade only
USER_SRC="u_src_${SUF}"    # grants on the source only
USER_DUAL="u_dual_${SUF}"  # grants on both
USER_PEEK="u_peek_${SUF}"  # facade grants + SHOW TABLES (only) on the source

${CLICKHOUSE_CLIENT} -m --query "
    DROP DATABASE IF EXISTS ${DB_OVL};
    DROP DATABASE IF EXISTS ${DB_SRC};
    DROP USER IF EXISTS ${USER_OVL}, ${USER_SRC}, ${USER_DUAL}, ${USER_PEEK};

    CREATE DATABASE ${DB_SRC};
    CREATE TABLE ${DB_SRC}.t (id UInt32, s String) ENGINE = MergeTree ORDER BY id;
    INSERT INTO ${DB_SRC}.t VALUES (1, 'a'), (2, 'b'), (150, 'big');
    CREATE VIEW ${DB_SRC}.v AS SELECT id, s FROM ${DB_SRC}.t WHERE id >= {min:UInt32};

    CREATE DATABASE ${DB_OVL} ENGINE = Overlay('${DB_SRC}');

    CREATE USER ${USER_OVL} IDENTIFIED WITH no_password;
    CREATE USER ${USER_SRC} IDENTIFIED WITH no_password;
    CREATE USER ${USER_DUAL} IDENTIFIED WITH no_password;
    CREATE USER ${USER_PEEK} IDENTIFIED WITH no_password;

    GRANT SELECT ON ${DB_OVL}.* TO ${USER_OVL};
    GRANT SELECT ON ${DB_SRC}.* TO ${USER_SRC};
    GRANT SELECT ON ${DB_OVL}.* TO ${USER_DUAL};
    GRANT SELECT ON ${DB_SRC}.* TO ${USER_DUAL};
    GRANT SELECT ON ${DB_OVL}.* TO ${USER_PEEK};
    GRANT SHOW TABLES ON ${DB_SRC}.* TO ${USER_PEEK};
"

function try
{
    local user="$1"
    local analyzer="$2"
    local query="$3"
    ${CLICKHOUSE_CLIENT} --user "${user}" --enable_analyzer "${analyzer}" --query "${query}" 2>&1 \
        | grep -o -m1 'UNKNOWN_FUNCTION\|ACCESS_DENIED' || true
}

# `EXPLAIN SYNTAX` inlines a parameterized view into the explained query, so it must not reveal the
# definition of the underlying source view without both grants. Report what the user gets: the
# inlined definition (only legitimate with both grants), a denial, or the unexpanded call.
function explain_syntax
{
    local user="$1"
    local analyzer="$2"
    local out
    out=$(${CLICKHOUSE_CLIENT} --user "${user}" --enable_analyzer "${analyzer}" \
        --query "EXPLAIN SYNTAX SELECT * FROM ${DB_OVL}.v(min = 2)" 2>&1)
    if echo "${out}" | grep -q 'ACCESS_DENIED'; then
        echo "ACCESS_DENIED"
    elif echo "${out}" | grep -q 'UNKNOWN_FUNCTION'; then
        echo "UNKNOWN_FUNCTION"
    elif echo "${out}" | grep -q "${DB_SRC}"; then
        echo "inlined"
    else
        echo "not inlined"
    fi
}

for analyzer in 0 1
do
    echo "=== enable_analyzer = ${analyzer} ==="

    echo "facade-only user: the view is indistinguishable from a missing one"
    try "${USER_OVL}" "${analyzer}" "SELECT * FROM ${DB_OVL}.v(min = 0) ORDER BY id"
    try "${USER_OVL}" "${analyzer}" "DESCRIBE TABLE ${DB_OVL}.v(min = 0)"

    echo "source-only user: denied on the facade side"
    try "${USER_SRC}" "${analyzer}" "SELECT * FROM ${DB_OVL}.v(min = 0) ORDER BY id"
    try "${USER_SRC}" "${analyzer}" "DESCRIBE TABLE ${DB_OVL}.v(min = 0)"

    echo "user with SHOW TABLES only on the source: sees the view, cannot read it"
    try "${USER_PEEK}" "${analyzer}" "SELECT * FROM ${DB_OVL}.v(min = 0) ORDER BY id"
    try "${USER_PEEK}" "${analyzer}" "DESCRIBE TABLE ${DB_OVL}.v(min = 0)"

    echo "dual-grant user: reads through the facade, the parameter applies"
    ${CLICKHOUSE_CLIENT} --user "${USER_DUAL}" --enable_analyzer "${analyzer}" \
        --query "SELECT * FROM ${DB_OVL}.v(min = 2) ORDER BY id"

    echo "dual-grant user: DESCRIBE through the facade"
    ${CLICKHOUSE_CLIENT} --user "${USER_DUAL}" --enable_analyzer "${analyzer}" \
        --query "DESCRIBE TABLE ${DB_OVL}.v(min = 0)" | cut -f1,2

    echo "EXPLAIN SYNTAX follows the same contract: only the dual-grant user sees the definition"
    explain_syntax "${USER_OVL}" "${analyzer}"
    explain_syntax "${USER_SRC}" "${analyzer}"
    explain_syntax "${USER_PEEK}" "${analyzer}"
    explain_syntax "${USER_DUAL}" "${analyzer}"
done

echo "=== row policies of the source view and of the facade are combined ==="
${CLICKHOUSE_CLIENT} -m --query "
    CREATE ROW POLICY p_src_${SUF} ON ${DB_SRC}.v FOR SELECT USING id < 100 TO ${USER_DUAL};
    CREATE ROW POLICY p_ovl_${SUF} ON ${DB_OVL}.v FOR SELECT USING id >= 2 TO ${USER_DUAL};
"
for analyzer in 0 1
do
    echo "-- direct source read applies the source policy only (analyzer = ${analyzer})"
    ${CLICKHOUSE_CLIENT} --user "${USER_DUAL}" --enable_analyzer "${analyzer}" \
        --query "SELECT * FROM ${DB_SRC}.v(min = 0) ORDER BY id"
    echo "-- the facade combines both policies (analyzer = ${analyzer})"
    ${CLICKHOUSE_CLIENT} --user "${USER_DUAL}" --enable_analyzer "${analyzer}" \
        --query "SELECT * FROM ${DB_OVL}.v(min = 0) ORDER BY id"
done

${CLICKHOUSE_CLIENT} -m --query "
    DROP ROW POLICY p_src_${SUF} ON ${DB_SRC}.v;
    DROP ROW POLICY p_ovl_${SUF} ON ${DB_OVL}.v;
    DROP USER ${USER_OVL}, ${USER_SRC}, ${USER_DUAL}, ${USER_PEEK};
    DROP DATABASE ${DB_OVL};
    DROP DATABASE ${DB_SRC};
"
