#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A dictionary created by a DDL query is also registered among tables, so a plain `EXISTS <name>`
# query can refer to a dictionary. For such a dictionary the `SHOW DICTIONARIES` privilege must be
# sufficient, matching `EXISTS DICTIONARY <name>` and what the documentation promises.
# https://github.com/ClickHouse/ClickHouse/issues/64364

DICT="${CLICKHOUSE_DATABASE}.test_dict_04402"
DICT_GRANTEE="dict_grantee_04402_${CLICKHOUSE_DATABASE}"
TABLE_GRANTEE="table_grantee_04402_${CLICKHOUSE_DATABASE}"
NOACCESS="noaccess_04402_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} -q "DROP DICTIONARY IF EXISTS ${DICT}"
${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${DICT_GRANTEE}, ${TABLE_GRANTEE}, ${NOACCESS}"

${CLICKHOUSE_CLIENT} -q "
    CREATE DICTIONARY ${DICT} (id UInt64, value String)
    PRIMARY KEY id
    SOURCE(CLICKHOUSE(TABLE 'numbers' DB 'system'))
    LIFETIME(MIN 0 MAX 300)
    LAYOUT(FLAT())
"

${CLICKHOUSE_CLIENT} -q "CREATE USER ${DICT_GRANTEE} IDENTIFIED WITH no_password"
${CLICKHOUSE_CLIENT} -q "GRANT SHOW DICTIONARIES ON ${DICT} TO ${DICT_GRANTEE}"

${CLICKHOUSE_CLIENT} -q "CREATE USER ${TABLE_GRANTEE} IDENTIFIED WITH no_password"
${CLICKHOUSE_CLIENT} -q "GRANT SHOW TABLES ON ${DICT} TO ${TABLE_GRANTEE}"

${CLICKHOUSE_CLIENT} -q "CREATE USER ${NOACCESS} IDENTIFIED WITH no_password"

echo "-- SHOW DICTIONARIES grantee, plain EXISTS (expect 1):"
${CLICKHOUSE_CLIENT} --user "${DICT_GRANTEE}" -q "EXISTS ${DICT}"

echo "-- SHOW DICTIONARIES grantee, EXISTS DICTIONARY (expect 1):"
${CLICKHOUSE_CLIENT} --user "${DICT_GRANTEE}" -q "EXISTS DICTIONARY ${DICT}"

echo "-- SHOW TABLES grantee, plain EXISTS (expect 1, unchanged behaviour):"
${CLICKHOUSE_CLIENT} --user "${TABLE_GRANTEE}" -q "EXISTS ${DICT}"

# A user with neither privilege must be denied, and the error must not reveal that the object is a
# dictionary (it asks for SHOW TABLES, the same as for a regular or non-existing table).
echo "-- no-access user, plain EXISTS (expect SHOW TABLES denial):"
${CLICKHOUSE_CLIENT} --user "${NOACCESS}" -q "EXISTS ${DICT}" 2>&1 | grep -oF "it's necessary to have the grant SHOW TABLES" | head -1

${CLICKHOUSE_CLIENT} -q "DROP DICTIONARY IF EXISTS ${DICT}"
${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${DICT_GRANTEE}, ${TABLE_GRANTEE}, ${NOACCESS}"
