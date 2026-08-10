#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A diagnostic raised through a read-only Overlay facade must name only the facade as it was written
# in the query. The source-side grant hides which source database owns a name, so an error text that
# spells out the resolved source id gives away exactly what that grant protects. This covers the two
# diagnostics that used to carry a source name: the source-side grant check of a read, and the
# runtime rejection of a facade that became nested through a late reconfiguration.

SUF="${CLICKHOUSE_TEST_UNIQUE_NAME}"
DB_SRC="db_src_${SUF}"
DB_OVL="db_ovl_${SUF}"
DB_HID="db_hid_${SUF}"
DB_TOP="db_top_${SUF}"
USER_OVL="u_ovl_${SUF}" # granted on the facades only, never on a source

${CLICKHOUSE_CLIENT} -nm --query "
    DROP DATABASE IF EXISTS ${DB_TOP};
    DROP DATABASE IF EXISTS ${DB_HID};
    DROP DATABASE IF EXISTS ${DB_OVL};
    DROP DATABASE IF EXISTS ${DB_SRC};
    DROP USER IF EXISTS ${USER_OVL};

    CREATE DATABASE ${DB_SRC} ENGINE = Atomic;
    CREATE TABLE ${DB_SRC}.t (id UInt64) ENGINE = MergeTree ORDER BY id;
    INSERT INTO ${DB_SRC}.t VALUES (1);

    CREATE DATABASE ${DB_OVL} ENGINE = Overlay('${DB_SRC}');

    CREATE USER ${USER_OVL} NOT IDENTIFIED;
    GRANT SELECT ON ${DB_OVL}.* TO ${USER_OVL};
    GRANT SHOW ON ${DB_OVL}.* TO ${USER_OVL};
"

# The client prints the error text more than once, so match it with `grep -q` instead of counting.
check_hidden()
{
    local label="$1"
    local hidden="$2"
    local err="$3"

    echo -n "${label}: "
    if [ -z "${err}" ]
    then
        echo 'FAIL (expected an error, got none)'
    elif echo "${err}" | grep -q "${hidden}"
    then
        echo 'FAIL (hidden source database name leaked)'
    else
        echo 'OK'
    fi
}

echo 'Sanity: the default user reads through the facade'
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM ${DB_OVL}.t"

echo 'A facade-only grant is denied without naming the source database'
for analyzer in 0 1
do
    err=$(${CLICKHOUSE_CLIENT} --user="${USER_OVL}" --query \
        "SELECT * FROM ${DB_OVL}.t SETTINGS enable_analyzer = ${analyzer}" 2>&1 >/dev/null)
    check_hidden "SELECT with enable_analyzer = ${analyzer}" "${DB_SRC}" "${err}"
done

err=$(${CLICKHOUSE_CLIENT} --user="${USER_OVL}" --query "DESCRIBE ${DB_OVL}.t" 2>&1 >/dev/null)
check_hidden 'DESCRIBE' "${DB_SRC}" "${err}"

err=$(${CLICKHOUSE_CLIENT} --user="${USER_OVL}" --query "SHOW CREATE TABLE ${DB_OVL}.t" 2>&1 >/dev/null)
check_hidden 'SHOW CREATE TABLE' "${DB_SRC}" "${err}"

# Build a facade whose source only becomes another facade afterwards: the check at CREATE time
# cannot see this, so the nesting is rejected lazily on the first lookup through the facade - on
# listing paths that run before the source-name masking of `SHOW CREATE DATABASE`.
${CLICKHOUSE_CLIENT} -nm --query "
    CREATE DATABASE ${DB_HID} ENGINE = Atomic;
    CREATE DATABASE ${DB_TOP} ENGINE = Overlay('${DB_HID}');
    DROP DATABASE ${DB_HID};
    CREATE DATABASE ${DB_HID} ENGINE = Overlay('${DB_SRC}');
    GRANT SHOW ON ${DB_TOP}.* TO ${USER_OVL};
    GRANT SELECT ON ${DB_TOP}.* TO ${USER_OVL};
"

echo 'A facade nested by a late reconfiguration is rejected without naming the source database'
err=$(${CLICKHOUSE_CLIENT} --user="${USER_OVL}" --query "SHOW TABLES FROM ${DB_TOP}" 2>&1 >/dev/null)
check_hidden 'SHOW TABLES' "${DB_HID}" "${err}"

err=$(${CLICKHOUSE_CLIENT} --user="${USER_OVL}" --query "SELECT * FROM ${DB_TOP}.t" 2>&1 >/dev/null)
check_hidden 'SELECT' "${DB_HID}" "${err}"

${CLICKHOUSE_CLIENT} -nm --query "
    DROP DATABASE IF EXISTS ${DB_TOP};
    DROP DATABASE IF EXISTS ${DB_HID};
    DROP DATABASE IF EXISTS ${DB_OVL};
    DROP DATABASE IF EXISTS ${DB_SRC};
    DROP USER IF EXISTS ${USER_OVL};
"
