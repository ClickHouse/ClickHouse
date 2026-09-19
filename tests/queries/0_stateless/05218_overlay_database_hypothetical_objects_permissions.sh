#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Hypothetical objects (`CREATE HYPOTHETICAL INDEX` / `CREATE HYPOTHETICAL PROJECTION`) resolve the
# written table name to a storage before they do anything, so through a read-only Overlay facade the
# lookup loads the underlying source table. The source-side grant must be proven before that lookup,
# fail-closed and naming only the facade, exactly as for the other table lookups through a facade:
#   - a user granted on the facade alone is rejected with ACCESS_DENIED, and the message does not
#     name the source database;
#   - a user granted on both sides gets past the access checks; the facade then resolves to a table
#     without a UUID (a read-only Overlay never answers with a source UUID), so the hypothetical
#     object is rejected as unsupported, while the same statements on the source itself work.

SUF="${CLICKHOUSE_TEST_UNIQUE_NAME}"

DB_SRC="db_src_${SUF}"
DB_OVL="dbovl_${SUF}"
T="t_hypo"

USER_OVL="u_hypo_ovl_${SUF}"   # grants on the facade only
USER_DUAL="u_hypo_dual_${SUF}" # grants on the facade and on the source

${CLICKHOUSE_CLIENT} -nm --query "
    DROP DATABASE IF EXISTS ${DB_OVL};
    DROP DATABASE IF EXISTS ${DB_SRC};
    DROP USER IF EXISTS ${USER_OVL};
    DROP USER IF EXISTS ${USER_DUAL};

    CREATE DATABASE ${DB_SRC} ENGINE = Atomic;
    CREATE TABLE ${DB_SRC}.${T} (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a;
    INSERT INTO ${DB_SRC}.${T} SELECT number, number % 10 FROM numbers(100);

    CREATE DATABASE ${DB_OVL} ENGINE = Overlay('${DB_SRC}');

    CREATE USER ${USER_OVL} NOT IDENTIFIED;
    CREATE USER ${USER_DUAL} NOT IDENTIFIED;

    GRANT ALTER ADD PROJECTION, ALTER ADD INDEX, SELECT ON ${DB_OVL}.* TO ${USER_OVL};

    GRANT ALTER ADD PROJECTION, ALTER ADD INDEX, SELECT ON ${DB_OVL}.* TO ${USER_DUAL};
    GRANT ALTER ADD PROJECTION, ALTER ADD INDEX, SELECT ON ${DB_SRC}.* TO ${USER_DUAL};
    GRANT SELECT ON system.hypothetical_indexes TO ${USER_DUAL};
    GRANT SELECT ON system.hypothetical_projections TO ${USER_DUAL};
"

# Prints the error code of a failed statement and whether the message names the source database.
function run_denied()
{
    local user="$1"
    local query="$2"
    local out
    out=$(${CLICKHOUSE_CLIENT} --user="${user}" --query "${query}" 2>&1)
    echo "${out}" | grep -o 'ACCESS_DENIED\|NOT_IMPLEMENTED' | uniq
    echo "source named: $(echo "${out}" | grep -c "${DB_SRC}")"
}

echo 'ALTER ADD PROJECTION on the Overlay database alone is not enough for a hypothetical projection through the facade'
run_denied "${USER_OVL}" "CREATE HYPOTHETICAL PROJECTION p_hypo ON ${DB_OVL}.${T} (SELECT a, b ORDER BY b)"

echo 'Dropping a hypothetical projection through the facade needs the source-side grant too'
run_denied "${USER_OVL}" "DROP HYPOTHETICAL PROJECTION IF EXISTS p_hypo ON ${DB_OVL}.${T}"

echo 'A user without any grant on the source cannot create a hypothetical index through the facade'
run_denied "${USER_OVL}" "CREATE HYPOTHETICAL INDEX i_hypo ON ${DB_OVL}.${T} (b) TYPE minmax GRANULARITY 1"

echo 'With grants on both sides the access checks pass; the facade has no table UUID, so the object is unsupported there'
run_denied "${USER_DUAL}" "CREATE HYPOTHETICAL PROJECTION p_hypo ON ${DB_OVL}.${T} (SELECT a, b ORDER BY b)"
run_denied "${USER_DUAL}" "CREATE HYPOTHETICAL INDEX i_hypo ON ${DB_OVL}.${T} (b) TYPE minmax GRANULARITY 1"

echo 'The same statements work on the underlying source table itself'
${CLICKHOUSE_CLIENT} --user="${USER_DUAL}" -nm --query "
    CREATE HYPOTHETICAL PROJECTION p_hypo ON ${DB_SRC}.${T} (SELECT a, b ORDER BY b);
    CREATE HYPOTHETICAL INDEX i_hypo ON ${DB_SRC}.${T} (b) TYPE minmax GRANULARITY 1;
    SELECT count() FROM system.hypothetical_projections WHERE database = '${DB_SRC}' AND table = '${T}';
    SELECT count() FROM system.hypothetical_indexes WHERE database = '${DB_SRC}' AND table = '${T}';
    DROP HYPOTHETICAL PROJECTION p_hypo ON ${DB_SRC}.${T};
    DROP HYPOTHETICAL INDEX i_hypo ON ${DB_SRC}.${T};
"

${CLICKHOUSE_CLIENT} -nm --query "
    DROP DATABASE IF EXISTS ${DB_OVL};
    DROP DATABASE IF EXISTS ${DB_SRC};
    DROP USER IF EXISTS ${USER_OVL};
    DROP USER IF EXISTS ${USER_DUAL};
"
