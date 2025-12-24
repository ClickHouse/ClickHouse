#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CLICKHOUSE_URL+="&input_format_skip_unknown_fields=0&insert_allow_alias_columns=1"

${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d "DROP TABLE IF EXISTS alias_insert_test"
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d "
CREATE TABLE alias_insert_test
(
    -- Base physical columns
    user_id UInt64,
    user_name String,
    rating Float64,

    -- Simple ALIAS columns (insertable)
    uid ALIAS user_id,
    login ALIAS user_name,

    -- Complex ALIAS columns (non-insertable)
    username_normalized ALIAS upper(user_name),
    rating_normalized ALIAS round(rating)
)
ENGINE = MergeTree()
ORDER BY user_id
"

# non-async INSERT
echo "INSERT INTO alias_insert_test VALUES (0, 'root', 0.)" | ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&async_insert=0" --data-binary @-
echo "INSERT INTO alias_insert_test (user_id, user_name, rating) VALUES (1, 'zeno',    10.)" | ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&async_insert=0" --data-binary @-
echo "INSERT INTO alias_insert_test (uid, login, rating)         VALUES (2, 'alice',   20.)" | ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&async_insert=0" --data-binary @-
echo "INSERT INTO alias_insert_test (user_id, login, rating)     VALUES (3, 'bob',     30.)" | ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&async_insert=0" --data-binary @-
echo "INSERT INTO alias_insert_test (uid, user_name, rating)     VALUES (4, 'charlie', 40.)" | ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&async_insert=0" --data-binary @-

# Custom formats
echo 'INSERT INTO alias_insert_test FORMAT JSONEachRow {"user_id": 5, "login": "john", "rating": 50.}' | ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&async_insert=0" --data-binary @-
echo 'INSERT INTO alias_insert_test FORMAT JSONEachRow {"uid": 6, "login": "mike", "rating": 60.}' | ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&async_insert=0" --data-binary @-

echo 'INSERT INTO alias_insert_test FORMAT JSONEachRow {"uid": 7, "login": "foo", "rating": 70., "user_name": "james"}' | ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&async_insert=0" --data-binary @- |& grep -m1 -o DUPLICATE_COLUMN

# negative cases
echo "INSERT INTO alias_insert_test (user_id, username_normalized, rating) VALUES (99, 'FAIL', 999.0)" | ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&async_insert=0" --data-binary @- |& grep -m1 -o NO_SUCH_COLUMN_IN_TABLE
echo "INSERT INTO alias_insert_test (user_id, user_name, rating_normalized) VALUES (99, 'FAIL', 999.0)" | ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&async_insert=0" --data-binary @- |& grep -m1 -o NO_SUCH_COLUMN_IN_TABLE

# async insert
echo "INSERT INTO alias_insert_test (user_id, user_name, rating) VALUES (10, 'zeno',  100.)" | ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&async_insert=1" --data-binary @-
echo "INSERT INTO alias_insert_test (uid, login, rating)         VALUES (20, 'alice', 200.)" | ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&async_insert=1" --data-binary @-
echo 'INSERT INTO alias_insert_test FORMAT JSONEachRow {"user_id": 50, "login": "john", "rating": 500.}' | ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&async_insert=1" --data-binary @-
echo 'INSERT INTO alias_insert_test FORMAT JSONEachRow {"uid": 60, "login": "mike", "rating": 600.}' | ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&async_insert=1" --data-binary @-
echo 'INSERT INTO alias_insert_test FORMAT JSONEachRow {"uid": 70, "login": "foo", "rating": 700., "user_name": "james"}' | ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&async_insert=1" --data-binary @- |& grep -o -m1 DUPLICATE_COLUMN

${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d "SELECT user_id, user_name, rating FROM alias_insert_test ORDER BY user_id"
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d "DROP TABLE IF EXISTS alias_insert_test"
