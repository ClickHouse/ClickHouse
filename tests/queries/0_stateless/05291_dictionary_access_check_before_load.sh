#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Access rights must be checked before a dictionary is loaded,
# so a query without the required grants does not trigger the load.

username="user_${CLICKHOUSE_TEST_UNIQUE_NAME}"

${CLICKHOUSE_CLIENT} -m --query "
    DROP USER IF EXISTS ${username};
    CREATE TABLE src (id UInt64, value String) ENGINE = Memory;
    INSERT INTO src VALUES (1, 'a');
    CREATE DICTIONARY d (id UInt64, value String)
    PRIMARY KEY id
    SOURCE(CLICKHOUSE(TABLE 'src' DB '${CLICKHOUSE_DATABASE}'))
    LAYOUT(FLAT())
    LIFETIME(0);
    CREATE USER ${username} NOT IDENTIFIED;
    GRANT CREATE TEMPORARY TABLE ON *.* TO ${username};
"

function status()
{
    ${CLICKHOUSE_CLIENT} --query "SELECT status FROM system.dictionaries WHERE database = currentDatabase() AND name = 'd'"
}

function unload()
{
    ${CLICKHOUSE_CLIENT} --query "SYSTEM UNLOAD DICTIONARY d"
    status
}

function as_user()
{
    echo "$1" | sed "s/${CLICKHOUSE_DATABASE}/db/g"
    local output
    output=$(${CLICKHOUSE_CLIENT} --user "${username}" --query "$1" 2>&1)
    if grep -q ACCESS_DENIED <<< "${output}"
    then
        echo ACCESS_DENIED
    else
        echo "${output}"
    fi
}

status

echo "--- no grants"
# Both a qualified name and an unqualified one resolved against the current database.
for dict in "${CLICKHOUSE_DATABASE}.d" "d"
do
    as_user "SELECT * FROM dictionary('${dict}')"
    status
    as_user "DESCRIBE TABLE dictionary('${dict}')"
    status
    as_user "SELECT dictGet('${dict}', 'value', toUInt64(1))"
    status
    as_user "SELECT dictHas('${dict}', toUInt64(1))"
    status
    as_user "SELECT count() FROM numbers(1) GROUP BY dictGet('${dict}', 'value', number) SETTINGS enable_analyzer = 0"
    status
done

echo "--- SELECT is enough for the dictionary table function, but not for dictGet"
${CLICKHOUSE_CLIENT} --query "GRANT SELECT ON ${CLICKHOUSE_DATABASE}.d TO ${username}"
as_user "SELECT dictGet('d', 'value', toUInt64(1))"
status
as_user "DESCRIBE TABLE dictionary('d')"
as_user "SELECT * FROM dictionary('d')"
status

echo "--- dictGet"
${CLICKHOUSE_CLIENT} -m --query "
    REVOKE SELECT ON ${CLICKHOUSE_DATABASE}.d FROM ${username};
    GRANT dictGet ON ${CLICKHOUSE_DATABASE}.d TO ${username};
"
unload
as_user "SELECT dictGet('d', 'value', toUInt64(1))"
status
unload
as_user "SELECT dictHas('${CLICKHOUSE_DATABASE}.d', toUInt64(1))"
status
unload
as_user "SELECT count() FROM numbers(1) GROUP BY dictGet('d', 'value', number) SETTINGS enable_analyzer = 0"
status
unload
as_user "SELECT * FROM dictionary('d')"
status

${CLICKHOUSE_CLIENT} --query "DROP USER ${username}"
