#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: needs the PostgreSQL and MySQL table engines, which the fast test does not build

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Enum and Nullable(Enum) columns of PostgreSQL and MySQL tables and Enum range columns of a PostgreSQL dictionary,
# pointed at this very server, are read as the declared Enum type: a remote value is taken as an element name, or
# as a number when it is not a name.

USER_NAME="pg_my_enum_${CLICKHOUSE_DATABASE}"
PG_HOST="localhost:${CLICKHOUSE_PORT_POSTGRESQL}"
MY_HOST="127.0.0.1:${CLICKHOUSE_PORT_MYSQL}"

echo "
DROP USER IF EXISTS ${USER_NAME};
CREATE USER ${USER_NAME} IDENTIFIED WITH plaintext_password BY 'pgpass';
GRANT SELECT ON ${CLICKHOUSE_DATABASE}.* TO ${USER_NAME};

CREATE TABLE src (k UInt32, name String, id Int64, n Nullable(String)) ENGINE = MergeTree ORDER BY k;
INSERT INTO src VALUES (1, 'a', 1, 'b'), (2, 'b', 2, NULL), (3, 'c', -1000, 'c');
CREATE TABLE src_int (k UInt32, c Nullable(Int64)) ENGINE = MergeTree ORDER BY k;
INSERT INTO src_int VALUES (1, 10);
CREATE TABLE src_range (id UInt64, lo Int64, hi Int64, v String) ENGINE = MergeTree ORDER BY id;
INSERT INTO src_range VALUES (1, 1, 2, 'one-two');

SELECT '--- PostgreSQL';
CREATE TABLE pg_enum (k UInt32, name Enum8('a' = 1, 'b' = 2, 'c' = 3), id Enum16('one' = 1, 'two' = 2, 'neg' = -1000), n Nullable(Enum8('b' = 1, 'c' = 2)))
ENGINE = PostgreSQL('${PG_HOST}', '${CLICKHOUSE_DATABASE}', 'src', '${USER_NAME}', 'pgpass');
SELECT k, name, id, n FROM pg_enum ORDER BY k FORMAT TSVWithNamesAndTypes;
SELECT arraySort(groupArray(name)), arraySort(groupArray(id)) FROM pg_enum;

SELECT '--- PostgreSQL dictionary with Enum range columns';
CREATE DICTIONARY pg_enum_dict (id UInt64, lo Enum8('p' = 1, 'q' = 2), hi Enum8('p' = 1, 'q' = 2), v String)
PRIMARY KEY id
SOURCE(POSTGRESQL(host '127.0.0.1' port ${CLICKHOUSE_PORT_POSTGRESQL} user '${USER_NAME}' password 'pgpass' db '${CLICKHOUSE_DATABASE}' table 'src_range'))
LAYOUT(RANGE_HASHED()) RANGE(MIN lo MAX hi) LIFETIME(0);
SELECT * FROM pg_enum_dict;

SELECT '--- MySQL';
CREATE TABLE my_enum (k UInt32, name Enum8('a' = 1, 'b' = 2, 'c' = 3), id Enum16('one' = 1, 'two' = 2, 'neg' = -1000))
ENGINE = MySQL('${MY_HOST}', '${CLICKHOUSE_DATABASE}', 'src', '${USER_NAME}', 'pgpass');
SELECT k, name, id FROM my_enum ORDER BY k FORMAT TSVWithNamesAndTypes;
SELECT arraySort(groupArray(name)), arraySort(groupArray(id)) FROM my_enum;

CREATE TABLE pg_enum_bad (k UInt32, c Nullable(Enum16('v0' = 0, 'v1' = 1, 'v2' = 2)))
ENGINE = PostgreSQL('${PG_HOST}', '${CLICKHOUSE_DATABASE}', 'src_int', '${USER_NAME}', 'pgpass');
CREATE TABLE my_enum_bad (k UInt32, c Enum16('v0' = 0, 'v1' = 1, 'v2' = 2))
ENGINE = MySQL('${MY_HOST}', '${CLICKHOUSE_DATABASE}', 'src_int', '${USER_NAME}', 'pgpass');
" | $CLICKHOUSE_CLIENT

echo "--- a value that is not an element of the enum is rejected"
ERROR=$($CLICKHOUSE_CLIENT --query "SELECT c FROM pg_enum_bad" 2>&1)
echo "$ERROR" | grep -q -F 'BAD_ARGUMENTS' && echo "$ERROR" | grep -q -F "Unknown element '10' for enum" && echo "rejected by PostgreSQL"
ERROR=$($CLICKHOUSE_CLIENT --query "SELECT c FROM my_enum_bad" 2>&1)
echo "$ERROR" | grep -q -F 'UNKNOWN_ELEMENT_OF_ENUM' && echo "$ERROR" | grep -q -F "Unknown element '10' for enum" && echo "rejected by MySQL"
$CLICKHOUSE_CLIENT --query "SELECT 1"

echo "
DROP TABLE my_enum_bad;
DROP TABLE pg_enum_bad;
DROP TABLE my_enum;
DROP DICTIONARY pg_enum_dict;
DROP TABLE pg_enum;
DROP TABLE src_range;
DROP TABLE src_int;
DROP TABLE src;
DROP USER ${USER_NAME};
" | $CLICKHOUSE_CLIENT
