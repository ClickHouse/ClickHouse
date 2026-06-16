#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The access check of `vectorSearch` must be built from source-table columns:
# the column behind the vector_similarity index is always required (the scorer
# reads it through the index), while virtual columns such as `_score` carry no
# source privileges.

user_name="${CLICKHOUSE_DATABASE}_test_user_04331"

$CLICKHOUSE_CLIENT -q "
DROP TABLE IF EXISTS tab_access;
DROP USER IF EXISTS $user_name;

CREATE TABLE tab_access(id Int32, val String, vec Array(Float32), INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 2))
ENGINE = MergeTree ORDER BY id;

INSERT INTO tab_access VALUES (0, 'a', [0.0, 0.0]), (1, 'b', [1.0, 0.0]), (2, 'c', [2.0, 0.0]);

CREATE USER $user_name IDENTIFIED WITH plaintext_password BY 'password';
"

function check_access()
{
    local output
    output=$($CLICKHOUSE_CLIENT --user "$user_name" --password "password" -q "$1" 2>&1)
    local rc=$?
    if [ $rc -eq 0 ]; then
        echo "OK: $output" | tr '\n' ' ' | sed 's/ $//'
        echo ""
    elif echo "$output" | grep -q "ACCESS_DENIED"; then
        echo "ACCESS_DENIED"
    else
        echo "$output"
    fi
}

query="SELECT id, _score FROM vectorSearch(currentDatabase(), tab_access, idx, [0.0, 0.0], 2) ORDER BY _score ASC, id SETTINGS allow_experimental_search_topk_table_functions = 1"

echo "-- no grants: denied"
check_access "$query"

echo "-- only SELECT(id): still denied, the scorer reads vec through the index"
$CLICKHOUSE_CLIENT -q "GRANT SELECT(id) ON $CLICKHOUSE_DATABASE.tab_access TO $user_name"
check_access "$query"

echo "-- SELECT(id, vec): works; _score needs no source grant"
$CLICKHOUSE_CLIENT -q "GRANT SELECT(vec) ON $CLICKHOUSE_DATABASE.tab_access TO $user_name"
check_access "$query"

echo "-- selecting an ungranted source column is denied"
check_access "SELECT val FROM vectorSearch(currentDatabase(), tab_access, idx, [0.0, 0.0], 2) SETTINGS allow_experimental_search_topk_table_functions = 1"

$CLICKHOUSE_CLIENT -q "
DROP TABLE IF EXISTS tab_access;
DROP USER IF EXISTS $user_name;
"
