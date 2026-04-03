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

(
    echo '{"user_id": 5, "login": "john", "rating": 50.}'
    echo '{"user_id": 5, "user_name": "mike", "rating": 50.}'
) | ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&query=INSERT+INTO+alias_insert_test+FORMAT+JSONEachRow" --data-binary @- |& (
    grep -o "Column 'login' is an ALIAS for 'user_name'. Cannot provide values for both columns"
)

exit 0
