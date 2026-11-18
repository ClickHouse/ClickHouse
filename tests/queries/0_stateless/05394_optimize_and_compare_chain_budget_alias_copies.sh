#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The `optimize_and_compare_chain_max_hash_work` budget must never make two copies of an expression (a SELECT alias and its GROUP BY key) be rewritten differently.

# Runs the statement once per budget and `enable_identifier_resolve_cache` value; SETTINGS_PLACEHOLDER marks where the settings go.
function sweep()
{
    for budget in $(seq 0 10 600); do
        for cache in 0 1; do
            echo "${1/SETTINGS_PLACEHOLDER/SETTINGS optimize_and_compare_chain = 1, optimize_and_compare_chain_max_hash_work = $budget, enable_identifier_resolve_cache = $cache} FORMAT TSV;"
        done
    done | $CLICKHOUSE_CLIENT | sort | uniq -c | sed 's/^ *//'
}

query="SELECT (number < (number + 1)) AND ((number + 1) < 3) AS r FROM numbers(5) GROUP BY r"

sweep "$query ORDER BY r SETTINGS_PLACEHOLDER"
sweep "$query SETTINGS_PLACEHOLDER"
# The derivation is applied to both copies or to none.
sweep "SELECT count() IN (0, 2) FROM (EXPLAIN QUERY TREE $query SETTINGS_PLACEHOLDER) WHERE explain ILIKE '%function_name: indexHint%'"

$CLICKHOUSE_CLIENT -q "SELECT count() FROM (EXPLAIN QUERY TREE $query SETTINGS optimize_and_compare_chain = 1, optimize_and_compare_chain_max_hash_work = 5000000) WHERE explain ILIKE '%function_name: indexHint%'"
