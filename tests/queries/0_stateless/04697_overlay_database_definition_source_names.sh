#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The definition of a read-only Overlay facade names every source database, so the
# database-metadata paths must not disclose it to a user who is not allowed to see those
# databases: SHOW CREATE DATABASE is denied, and system.databases.engine_full reports the bare
# `Overlay` engine instead of the member list. A user granted on every source sees the full
# definition.

SUF="${CLICKHOUSE_TEST_UNIQUE_NAME}"
DB_A="db_a_${SUF}"
DB_B="db_b_${SUF}"
DB_OVL="db_ovl_${SUF}"
USER_OVL="u_ovl_${SUF}"     # granted on the facade only
USER_PART="u_part_${SUF}"   # granted on the facade and on one of the two sources
USER_ALL="u_all_${SUF}"     # granted on the facade and on both sources

${CLICKHOUSE_CLIENT} -nm --query "
    DROP DATABASE IF EXISTS ${DB_OVL};
    DROP DATABASE IF EXISTS ${DB_A};
    DROP DATABASE IF EXISTS ${DB_B};
    DROP USER IF EXISTS ${USER_OVL};
    DROP USER IF EXISTS ${USER_PART};
    DROP USER IF EXISTS ${USER_ALL};

    CREATE DATABASE ${DB_A} ENGINE = Atomic;
    CREATE DATABASE ${DB_B} ENGINE = Atomic;
    CREATE TABLE ${DB_A}.t (x UInt64) ENGINE = MergeTree ORDER BY x;

    CREATE DATABASE ${DB_OVL} ENGINE = Overlay('${DB_A}', '${DB_B}');

    CREATE USER ${USER_OVL} NOT IDENTIFIED;
    CREATE USER ${USER_PART} NOT IDENTIFIED;
    CREATE USER ${USER_ALL} NOT IDENTIFIED;

    GRANT SHOW ON ${DB_OVL}.* TO ${USER_OVL};

    GRANT SHOW ON ${DB_OVL}.* TO ${USER_PART};
    GRANT SHOW ON ${DB_A}.* TO ${USER_PART};

    GRANT SHOW ON ${DB_OVL}.* TO ${USER_ALL};
    GRANT SHOW ON ${DB_A}.* TO ${USER_ALL};
    GRANT SHOW ON ${DB_B}.* TO ${USER_ALL};
"

# Reports what the given user's `SHOW CREATE DATABASE` discloses. The denial message must not name
# a source either, so the whole output — the definition or the error — is checked for the names.
show_create()
{
    local user="$1"
    local out
    out=$(${CLICKHOUSE_CLIENT} --user="${user}" --query "SHOW CREATE DATABASE ${DB_OVL}" 2>&1)
    if echo "${out}" | grep -q -e "${DB_A}" -e "${DB_B}"; then
        echo "definition with the source names"
    elif echo "${out}" | grep -q "ACCESS_DENIED"; then
        echo "denied"
    else
        echo "definition without the source names"
    fi
}

# Same for the `engine_full` column of `system.databases`, which formats the same definition.
engine_full()
{
    local user="$1"
    local out
    out=$(${CLICKHOUSE_CLIENT} --user="${user}" --query "SELECT engine_full FROM system.databases WHERE name = '${DB_OVL}'" 2>&1)
    if echo "${out}" | grep -q -e "${DB_A}" -e "${DB_B}"; then
        echo "leaks the source names"
    else
        echo "engine_full: ${out}"
    fi
}

echo 'Sanity: the default user sees the full definition'
${CLICKHOUSE_CLIENT} --query "SHOW CREATE DATABASE ${DB_OVL}" | sed -e "s/${DB_OVL}/db_ovl/" -e "s/${DB_A}/db_a/" -e "s/${DB_B}/db_b/"
${CLICKHOUSE_CLIENT} --query "SELECT engine_full FROM system.databases WHERE name = '${DB_OVL}'" | sed -e "s/${DB_A}/db_a/" -e "s/${DB_B}/db_b/"

echo 'Facade-only grant: SHOW CREATE DATABASE is denied'
show_create "${USER_OVL}"
echo 'Facade-only grant: system.databases still lists the facade, with the source names redacted'
engine_full "${USER_OVL}"

echo 'One source granted out of two: still denied, and still redacted'
show_create "${USER_PART}"
engine_full "${USER_PART}"

echo 'All sources granted: the definition is shown'
show_create "${USER_ALL}"
${CLICKHOUSE_CLIENT} --user="${USER_ALL}" --query "SELECT engine_full FROM system.databases WHERE name = '${DB_OVL}'" | sed -e "s/${DB_A}/db_a/" -e "s/${DB_B}/db_b/"

echo 'The scan of system.databases does not fail for a restricted user'
${CLICKHOUSE_CLIENT} --user="${USER_OVL}" --query "SELECT count() > 0 FROM system.databases"

${CLICKHOUSE_CLIENT} -nm --query "
    DROP DATABASE IF EXISTS ${DB_OVL};
    DROP DATABASE IF EXISTS ${DB_A};
    DROP DATABASE IF EXISTS ${DB_B};
    DROP USER IF EXISTS ${USER_OVL};
    DROP USER IF EXISTS ${USER_PART};
    DROP USER IF EXISTS ${USER_ALL};
"
