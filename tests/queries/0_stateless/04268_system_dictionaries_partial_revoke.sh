#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Test that system.dictionaries shows dictionaries when SHOW DICTIONARIES
# has a partial revoke on an unrelated table.

USER="test_user_04268_${CLICKHOUSE_DATABASE}"
ROLE="test_role_04268_${CLICKHOUSE_DATABASE}"
DICT="${CLICKHOUSE_DATABASE}.test_dict_04268"

${CLICKHOUSE_CLIENT} -q "DROP DICTIONARY IF EXISTS ${DICT}"
${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${USER}"
${CLICKHOUSE_CLIENT} -q "DROP ROLE IF EXISTS ${ROLE}"

${CLICKHOUSE_CLIENT} -q "
    CREATE DICTIONARY ${DICT} (id UInt64, value String)
    PRIMARY KEY id
    SOURCE(CLICKHOUSE(TABLE 'numbers' DB 'system'))
    LIFETIME(MIN 0 MAX 300)
    LAYOUT(FLAT())
"

# Create role with SHOW DICTIONARIES and a partial revoke on an unrelated table
${CLICKHOUSE_CLIENT} -q "CREATE ROLE ${ROLE}"
${CLICKHOUSE_CLIENT} -q "GRANT SHOW DICTIONARIES, SELECT ON *.* TO ${ROLE}"
${CLICKHOUSE_CLIENT} -q "REVOKE SHOW DICTIONARIES, SELECT ON system.non_existing_table FROM ${ROLE}"

${CLICKHOUSE_CLIENT} -q "CREATE USER ${USER} IDENTIFIED WITH no_password"
${CLICKHOUSE_CLIENT} -q "GRANT ${ROLE} TO ${USER}"

# The dictionary should be visible despite the partial revoke on system.billing
${CLICKHOUSE_CLIENT} --user "${USER}" -q "SELECT name FROM system.dictionaries WHERE database = '${CLICKHOUSE_DATABASE}' AND name = 'test_dict_04268'"

# Cleanup
${CLICKHOUSE_CLIENT} -q "DROP DICTIONARY IF EXISTS ${DICT}"
${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${USER}"
${CLICKHOUSE_CLIENT} -q "DROP ROLE IF EXISTS ${ROLE}"
