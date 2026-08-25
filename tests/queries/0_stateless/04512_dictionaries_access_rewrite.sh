#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -euo pipefail

user_name="user_04512_mira_${CLICKHOUSE_DATABASE}"
table_name="table_04512_${CLICKHOUSE_DATABASE}"
dict_name="dict_04512_${CLICKHOUSE_DATABASE}"

create_query="
    CREATE DICTIONARY ${dict_name}(x Int32, y Int32) PRIMARY KEY x
    LAYOUT(FLAT())
    SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE '${table_name}' DB '${CLICKHOUSE_DATABASE}'))
    LIFETIME(0)
"

drop_query="DROP DICTIONARY ${dict_name}"

query_as_user()
{
    local query="$1"
    $CLICKHOUSE_CLIENT_BINARY --database="${CLICKHOUSE_DATABASE}" --user="${user_name}" --query="$query"
}

error_as_user()
{
    local query="$1"
    $CLICKHOUSE_CLIENT_BINARY --database="${CLICKHOUSE_DATABASE}" --user="${user_name}" --query="$query" 2>&1 || true
}

cleanup()
{
    $CLICKHOUSE_CLIENT --query "
        DROP DICTIONARY IF EXISTS ${dict_name};
        DROP TABLE IF EXISTS ${table_name};
        DROP USER IF EXISTS ${user_name};
    "
}

trap cleanup EXIT
cleanup

$CLICKHOUSE_CLIENT --query "
    CREATE USER ${user_name};
    CREATE TABLE ${table_name}(x Int32, y Int32) ENGINE=Log;
    INSERT INTO ${table_name} VALUES (5, 6);
"

echo "create_dictionary"
[ "$($CLICKHOUSE_CLIENT --query "SHOW GRANTS FOR ${user_name}")" = "" ]
grep -q "Not enough privileges" <<<"$(error_as_user "$create_query")"
$CLICKHOUSE_CLIENT --query "GRANT CREATE DICTIONARY ON *.* TO ${user_name}"
query_as_user "$create_query"
$CLICKHOUSE_CLIENT --query "$drop_query"
$CLICKHOUSE_CLIENT --query "REVOKE CREATE DICTIONARY ON *.* FROM ${user_name}"
[ "$($CLICKHOUSE_CLIENT --query "SHOW GRANTS FOR ${user_name}")" = "" ]
grep -q "Not enough privileges" <<<"$(error_as_user "$create_query")"
$CLICKHOUSE_CLIENT --query "GRANT CREATE DICTIONARY ON ${CLICKHOUSE_DATABASE}.* TO ${user_name}"
query_as_user "$create_query"
$CLICKHOUSE_CLIENT --query "$drop_query"
$CLICKHOUSE_CLIENT --query "REVOKE CREATE DICTIONARY ON ${CLICKHOUSE_DATABASE}.* FROM ${user_name}"
[ "$($CLICKHOUSE_CLIENT --query "SHOW GRANTS FOR ${user_name}")" = "" ]
grep -q "Not enough privileges" <<<"$(error_as_user "$create_query")"
$CLICKHOUSE_CLIENT --query "GRANT CREATE DICTIONARY ON ${CLICKHOUSE_DATABASE}.${dict_name} TO ${user_name}"
query_as_user "$create_query"
echo "OK"

echo "drop_dictionary"
grep -Fq "GRANT CREATE DICTIONARY ON ${CLICKHOUSE_DATABASE}.${dict_name} TO ${user_name}" <<<"$($CLICKHOUSE_CLIENT --query "SHOW GRANTS FOR ${user_name}")"
grep -q "Not enough privileges" <<<"$(error_as_user "$drop_query")"
$CLICKHOUSE_CLIENT --query "GRANT DROP DICTIONARY ON *.* TO ${user_name}"
query_as_user "$drop_query"
$CLICKHOUSE_CLIENT --query "$create_query"
echo "OK"

echo "dictget"
[ "$($CLICKHOUSE_CLIENT --query "SELECT dictGet('${CLICKHOUSE_DATABASE}.${dict_name}', 'y', toUInt64(5))")" = "6" ]
grep -q "Not enough privileges" <<<"$(error_as_user "SELECT dictGet('${CLICKHOUSE_DATABASE}.${dict_name}', 'y', toUInt64(5))")"
$CLICKHOUSE_CLIENT --query "GRANT dictGet ON ${CLICKHOUSE_DATABASE}.${dict_name} TO ${user_name}"
[ "$(query_as_user "SELECT dictGet('${CLICKHOUSE_DATABASE}.${dict_name}', 'y', toUInt64(5))")" = "6" ]
[ "$($CLICKHOUSE_CLIENT --query "SELECT dictGet('${CLICKHOUSE_DATABASE}.${dict_name}', 'y', toUInt64(1))")" = "0" ]
[ "$(query_as_user "SELECT dictGet('${CLICKHOUSE_DATABASE}.${dict_name}', 'y', toUInt64(1))")" = "0" ]
$CLICKHOUSE_CLIENT --query "REVOKE dictGet ON *.* FROM ${user_name}"
grep -q "Not enough privileges" <<<"$(error_as_user "SELECT dictGet('${CLICKHOUSE_DATABASE}.${dict_name}', 'y', toUInt64(1))")"
echo "OK"
