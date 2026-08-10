#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `vectorSearch` discloses source-table metadata before the source-table SELECT check in
# `StorageMergeTreeScoredSearchBase::read`: the structure it returns (`DESCRIBE` never reaches
# `read` at all) and, while resolving the index, whether an index of a given name exists, whether
# it is a `vector_similarity` one, and the names of all vector indexes. A user with no grants on
# the source table must get ACCESS_DENIED instead of any of that.

user_name="${CLICKHOUSE_DATABASE}_test_user_04812"

$CLICKHOUSE_CLIENT -q "
DROP TABLE IF EXISTS tab_metadata;
DROP USER IF EXISTS $user_name;

CREATE TABLE tab_metadata(id Int32, secret String, vec Array(Float32), INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 2))
ENGINE = MergeTree ORDER BY id;

INSERT INTO tab_metadata VALUES (0, 'a', [0.0, 0.0]), (1, 'b', [1.0, 0.0]);

CREATE USER $user_name IDENTIFIED WITH plaintext_password BY 'password';
"

function run_as_user()
{
    local output
    output=$($CLICKHOUSE_CLIENT --user "$user_name" --password "password" -q "$1 SETTINGS allow_experimental_search_topk_table_functions = 1" 2>&1)
    local rc=$?

    if [ $rc -eq 0 ]; then
        echo "$output"
    elif echo "$output" | grep -q "ACCESS_DENIED"; then
        echo "ACCESS_DENIED"
    else
        echo "$output"
    fi
}

echo "-- no grants: the structure is not disclosed"
run_as_user "DESCRIBE vectorSearch(currentDatabase(), tab_metadata, idx, [0.0, 0.0], 1)" | cut -f1,2

echo "-- no grants: the existence of an index is not disclosed"
run_as_user "SELECT count() FROM vectorSearch(currentDatabase(), tab_metadata, no_such_index, [0.0, 0.0], 1)"

echo "-- no grants: the names of the vector indexes are not disclosed"
run_as_user "SELECT count() FROM vectorSearch(currentDatabase(), tab_metadata, [0.0, 0.0], 1)"

$CLICKHOUSE_CLIENT -q "GRANT SELECT(id, vec) ON $CLICKHOUSE_DATABASE.tab_metadata TO $user_name"

echo "-- column-level grants are enough to search"
run_as_user "SELECT id FROM vectorSearch(currentDatabase(), tab_metadata, idx, [0.0, 0.0], 1)"

echo "-- column-level grants do not disclose the structure, just like DESCRIBE of the source table"
run_as_user "DESCRIBE TABLE tab_metadata" | cut -f1,2
run_as_user "DESCRIBE vectorSearch(currentDatabase(), tab_metadata, idx, [0.0, 0.0], 1)" | cut -f1,2

$CLICKHOUSE_CLIENT -q "GRANT SELECT ON $CLICKHOUSE_DATABASE.tab_metadata TO $user_name"

echo "-- table-level grant: the structure is disclosed"
run_as_user "DESCRIBE vectorSearch(currentDatabase(), tab_metadata, idx, [0.0, 0.0], 1)" | cut -f1,2

$CLICKHOUSE_CLIENT -q "
DROP TABLE IF EXISTS tab_metadata;
DROP USER IF EXISTS $user_name;
"
