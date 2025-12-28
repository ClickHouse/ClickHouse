#!/usr/bin/env bash
# Tags: no-parallel-replicas

# Tests text search setting 'query_plan_text_index_add_hint' with functions mapKeys and mapValues

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

MY_CLICKHOUSE_CLIENT="${CLICKHOUSE_CLIENT} --enable_full_text_index 1 --use_skip_indexes_on_data_read 1 --query_plan_text_index_add_hint 1 --use_query_condition_cache 0 --enable_analyzer 1"

$MY_CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS tab;

    CREATE TABLE tab
    (
        m Map(String, String),
        INDEX idx_mk (mapKeys(m)) TYPE text(tokenizer = splitByNonAlpha) GRANULARITY 4,
        INDEX idx_mv (mapValues(m)) TYPE text(tokenizer = splitByNonAlpha) GRANULARITY 4
    ) ENGINE = MergeTree ORDER BY tuple();

    INSERT INTO tab SELECT (arrayMap(x -> 'k' || x, range(number % 30)), arrayMap(x -> 'v' || x, range(number % 30))) FROM numbers(100000);
"

function run()
{
    query=$1
    echo "$query"
    $MY_CLICKHOUSE_CLIENT --query "$query"

    $MY_CLICKHOUSE_CLIENT --query "
        SELECT trim(explain) AS str FROM
        (
            EXPLAIN actions = 1, indexes = 1 $query SETTINGS use_skip_indexes_on_data_read = 1
        )
        WHERE explain ILIKE '%filter column%' OR explain ILIKE '%name: idx%'
        ORDER BY str;
    "
}

run "SELECT count() FROM tab WHERE has(mapKeys(m), 'k18')"
run "SELECT count() FROM tab WHERE has(m, 'k18')"
run "SELECT count() FROM tab WHERE mapContains(m, 'k18')"
run "SELECT count() FROM tab WHERE mapContainsKey(m, 'k18')"
run "SELECT count() FROM tab WHERE mapContainsKeyLike(m, '%k18%')"
run "SELECT count() FROM tab WHERE mapContainsValue(m, 'v18')"
run "SELECT count() FROM tab WHERE mapContainsValueLike(m, '%v18%')"
run "SELECT count() FROM tab WHERE m['k18'] = 'v18'"
run "SELECT count() FROM tab WHERE m['k18'] LIKE '%v18%'"
run "SELECT count() FROM tab WHERE notEmpty(m['k18'])"
run "SELECT count() FROM tab WHERE empty(m['k18'])"
run "SELECT count() FROM tab WHERE toUInt64OrZero(extract(m['k18'], '[0-9]+')) = 18"
run "SELECT count() FROM tab WHERE toUInt64OrZero(extract(m['k18'], '[0-9]+')) = 0"

$MY_CLICKHOUSE_CLIENT --query "DROP TABLE tab;"
