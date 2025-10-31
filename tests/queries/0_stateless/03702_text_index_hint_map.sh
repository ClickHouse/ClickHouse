#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Depends on Parquet

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

MY_CLICKHOUSE_CLIENT="${CLICKHOUSE_CLIENT} --allow_experimental_full_text_index 1 --use_skip_indexes_on_data_read 1 --query_plan_text_index_add_hint 1 --use_query_condition_cache 0"

$MY_CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS t_text_index_hint_map;

    CREATE TABLE t_text_index_hint_map
    (
        m Map(String, String),
        INDEX idx_mk (mapKeys(m)) TYPE text(tokenizer = array) GRANULARITY 4,
        INDEX idx_mv (mapValues(m)) TYPE text(tokenizer = array) GRANULARITY 4
    ) ENGINE = MergeTree ORDER BY tuple();

    INSERT INTO t_text_index_hint_map SELECT (arrayMap(x -> 'k' || x, range(number % 30)), arrayMap(x -> 'v' || x, range(number % 30))) FROM numbers(100000);
"

function run()
{
    query=$1
    echo "$query"
    $MY_CLICKHOUSE_CLIENT --query "$query"

    $MY_CLICKHOUSE_CLIENT --query "
        SELECT trim(explain) FROM
        (
            EXPLAIN actions = 1, indexes = 1 $query
        )
        WHERE explain ILIKE '%filter column%' OR explain ILIKE '%name: idx%'
    "
}

run "SELECT count() FROM t_text_index_hint_map WHERE has(mapKeys(m), 'k18')"
run "SELECT count() FROM t_text_index_hint_map WHERE has(m, 'k18')"
run "SELECT count() FROM t_text_index_hint_map WHERE mapContainsKey(m, 'k18')"
run "SELECT count() FROM t_text_index_hint_map WHERE mapContainsValue(m, 'v18')"
run "SELECT count() FROM t_text_index_hint_map WHERE m['k18'] = 'v18'"
run "SELECT count() FROM t_text_index_hint_map WHERE m['k18'] LIKE '%v18%'"
run "SELECT count() FROM t_text_index_hint_map WHERE notEmpty(m['k18'])"
run "SELECT count() FROM t_text_index_hint_map WHERE empty(m['k18'])"
run "SELECT count() FROM t_text_index_hint_map WHERE toUInt64OrZero(extract(m['k18'], '[0-9]+')) = 18"
run "SELECT count() FROM t_text_index_hint_map WHERE toUInt64OrZero(extract(m['k18'], '[0-9]+')) = 0"

$MY_CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS t_text_index_hint_map;"
