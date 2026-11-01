#!/usr/bin/env bash
# Tags: no-parallel-replicas

# Many `OR`ed `LIKE` patterns must not build an unbounded number of dictionary DFAs: the DFAs of one query share a memory
# budget, and patterns without a DFA use the regular dictionary scan. The result must stay correct, and the query must
# fit into a small memory limit (each of these patterns used to build ~1 MiB DFA, 500 of them needed ~600 MiB).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "
DROP TABLE IF EXISTS text_dfa_budget;
CREATE TABLE text_dfa_budget (id UInt64, body String, INDEX idx body TYPE text(tokenizer = array, dictionary_block_size = 128))
ENGINE = MergeTree ORDER BY id;
INSERT INTO text_dfa_budget SELECT number, concat(if(number % 1000 = 0, 'service', 'unrelated'), toString(number), 'error') FROM numbers(100000);
"

# 'service%a' followed by 8 '_' has a large DFA, because every 'a' starts a new match attempt.
WHERE=$(for i in $(seq 1 400); do printf "body LIKE 'service%%a________%d' OR " "$i"; done)
WHERE="${WHERE} body LIKE 'service1000error'"

SETTINGS="use_skip_indexes = 1, use_query_condition_cache = 0, optimize_rewrite_like_perfect_affix = 0, use_text_index_like_evaluation_by_dictionary_scan = 1, max_threads = 1"

echo "index on, memory limited"
$CLICKHOUSE_CLIENT --max_memory_usage 100000000 -q "SELECT count() FROM text_dfa_budget WHERE ${WHERE} SETTINGS ${SETTINGS}"
echo "index off"
$CLICKHOUSE_CLIENT -q "SELECT count() FROM text_dfa_budget WHERE ${WHERE} SETTINGS ${SETTINGS}, use_skip_indexes = 0"

$CLICKHOUSE_CLIENT -q "DROP TABLE text_dfa_budget"
