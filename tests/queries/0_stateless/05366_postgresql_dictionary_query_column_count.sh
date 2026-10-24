#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: needs the PostgreSQL dictionary source, which is built only with libpqxx

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A PostgreSQL dictionary pointed at this very server, whose QUERY returns fewer or more columns than the
# dictionary structure, fails to load with a typed error that names the column order the QUERY must use.

USER_NAME="pg_dict_cols_${CLICKHOUSE_DATABASE}"
PG_SOURCE="host '127.0.0.1' port ${CLICKHOUSE_PORT_POSTGRESQL} user '${USER_NAME}' password 'pgpass' db '${CLICKHOUSE_DATABASE}'"

echo "
DROP USER IF EXISTS ${USER_NAME};
CREATE USER ${USER_NAME} IDENTIFIED WITH plaintext_password BY 'pgpass';
GRANT SELECT ON ${CLICKHOUSE_DATABASE}.* TO ${USER_NAME};

CREATE TABLE src (id UInt64, name String, extra String) ENGINE = MergeTree ORDER BY id;
INSERT INTO src VALUES (1, 'a', 'x');

CREATE DICTIONARY dict_too_few (id UInt64, name String, extra String)
PRIMARY KEY id
SOURCE(POSTGRESQL(${PG_SOURCE} query 'SELECT id, name FROM src'))
LAYOUT(FLAT()) LIFETIME(0);

CREATE DICTIONARY dict_too_many (id UInt64, name String)
PRIMARY KEY id
SOURCE(POSTGRESQL(${PG_SOURCE} query 'SELECT id, name, extra FROM src'))
LAYOUT(FLAT()) LIFETIME(0);
" | $CLICKHOUSE_CLIENT

function assert_error()
{
    local output
    output=$($CLICKHOUSE_CLIENT -q "$1" 2>&1)
    shift
    for fragment in "$@"; do
        grep -q -F -- "$fragment" <<< "$output" && echo "$fragment" || echo "missing '$fragment' in: $output"
    done
}

echo "--- too few columns"
assert_error "SELECT dictGet('dict_too_few', 'name', toUInt64(1))" \
    '(NUMBER_OF_COLUMNS_DOESNT_MATCH)' 'Row has 2 columns instead of 3' 'must return them in this order: `id`, `name`, `extra`'

echo "--- too many columns"
assert_error "SELECT dictGet('dict_too_many', 'name', toUInt64(1))" \
    '(TOO_MANY_COLUMNS)' 'Row has 3 columns instead of 2' 'must return them in this order: `id`, `name`'

echo "
DROP DICTIONARY dict_too_few;
DROP DICTIONARY dict_too_many;
DROP TABLE src;
DROP USER ${USER_NAME};
" | $CLICKHOUSE_CLIENT
