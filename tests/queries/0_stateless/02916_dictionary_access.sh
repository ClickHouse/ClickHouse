#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

username="user_${CLICKHOUSE_TEST_UNIQUE_NAME}"
dictname="dict_${CLICKHOUSE_TEST_UNIQUE_NAME}"

${CLICKHOUSE_CLIENT} -m --query "
    CREATE DICTIONARY IF NOT EXISTS ${dictname}
    (
        id UInt64,
        value UInt64
    )
    PRIMARY KEY id
    SOURCE(NULL())
    LAYOUT(FLAT())
    LIFETIME(MIN 0 MAX 1000);
    CREATE USER IF NOT EXISTS ${username} NOT IDENTIFIED;
    GRANT CREATE TEMPORARY TABLE ON *.* to ${username};
    SELECT * FROM dictionary(${dictname});
    SELECT dictGet(${dictname}, 'value', 1);
"

$CLICKHOUSE_CLIENT -m --user="${username}" --query "
    SELECT * FROM dictionary(${dictname});
" 2>&1 | grep -o ACCESS_DENIED | uniq

$CLICKHOUSE_CLIENT -m --user="${username}" --query "
    SELECT dictGet(${dictname}, 'value', 1);
" 2>&1 | grep -o ACCESS_DENIED | uniq

${CLICKHOUSE_CLIENT} -m --query "
    DROP DICTIONARY IF EXISTS ${dictname};
    DROP USER IF EXISTS ${username};
"
