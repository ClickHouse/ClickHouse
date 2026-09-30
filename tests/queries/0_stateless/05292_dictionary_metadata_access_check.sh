#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Without the `dictGet` grant, the dictionary definition (attributes, layout) must not be revealed,
# even when a query only needs the result type and does not execute the function.

username="user_${CLICKHOUSE_TEST_UNIQUE_NAME}"

${CLICKHOUSE_CLIENT} -m --query "
    DROP USER IF EXISTS ${username};
    CREATE TABLE src (id UInt64, value String) ENGINE = Memory;
    CREATE DICTIONARY d (id UInt64, value String)
    PRIMARY KEY id
    SOURCE(CLICKHOUSE(TABLE 'src' DB '${CLICKHOUSE_DATABASE}'))
    LAYOUT(FLAT())
    LIFETIME(0);
    CREATE USER ${username} NOT IDENTIFIED;
    GRANT CREATE TEMPORARY TABLE ON *.* TO ${username};
"

function as_user()
{
    echo "$1"
    local output
    output=$(${CLICKHOUSE_CLIENT} --user "${username}" --query "$1" 2>&1)
    if grep -q ACCESS_DENIED <<< "${output}"
    then
        echo ACCESS_DENIED
    else
        echo "${output}"
    fi
}

echo "--- no grants"
as_user "SELECT dictGet('d', 'no_such_attribute', toUInt64(1))"
as_user "DESCRIBE (SELECT dictGet('d', 'value', toUInt64(1)))"
as_user "SELECT dictGet('d', 'value', number) FROM numbers(0)"
as_user "SELECT dictGetOrDefault('d', 'value', toUInt64(1), 'x')"
as_user "SELECT dictGetOrNull('d', 'value', toUInt64(1))"
as_user "SELECT dictGetKeys('d', 'value', 'a')"
as_user "SELECT dictGetKeys('d', 'no_such_attribute', 'a')"
as_user "SELECT naiveBayesClassifier('d', 'good')"

echo "--- dictGet"
${CLICKHOUSE_CLIENT} --query "GRANT dictGet ON ${CLICKHOUSE_DATABASE}.d TO ${username}"
as_user "DESCRIBE (SELECT dictGet('d', 'value', toUInt64(1)))"
as_user "SELECT dictGet('d', 'no_such_attribute', toUInt64(1))" | sed 's/^ACCESS_DENIED$/unexpected ACCESS_DENIED/' | grep -o -E "^SELECT.*|No such attribute|unexpected ACCESS_DENIED" | head -2
as_user "SELECT dictGetKeys('d', 'no_such_attribute', 'a')" | sed 's/^ACCESS_DENIED$/unexpected ACCESS_DENIED/' | grep -o -E "^SELECT.*|has no attribute|unexpected ACCESS_DENIED" | head -2
as_user "SELECT naiveBayesClassifier('d', 'good')" | sed 's/^ACCESS_DENIED$/unexpected ACCESS_DENIED/' | grep -o -E "^SELECT.*|NAIVE_BAYES layout|unexpected ACCESS_DENIED" | head -2

${CLICKHOUSE_CLIENT} --query "DROP USER ${username}"
