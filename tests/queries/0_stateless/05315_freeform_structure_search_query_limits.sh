#!/usr/bin/env bash

# An unbounded search for the structure of a `Freeform` row (`input_format_freeform_max_search_steps = 0`) stops at
# the memory limit, the time limit and `KILL QUERY`, and so does a long validation of the candidates.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FILE_NAME=${CLICKHOUSE_TEST_UNIQUE_NAME}.data
DATA_FILE=${USER_FILES_PATH:?}/$FILE_NAME

trap 'rm -f "$DATA_FILE"' EXIT

# Writes two rows of N one-letter words separated by tabs: the number of candidate structures is exponential in N.
function words_row()
{
    local row
    row=$(yes a | head -n "$1" | paste -sd $'\t' -)
    for _ in 1 2; do echo "$row"; done > "$DATA_FILE"
}

echo "An unbounded search stops at the memory limit"
# Every candidate of a wide row has many columns, so the limit is reached after a few steps of the search.
words_row 240
$CLICKHOUSE_CLIENT -q "desc file('$FILE_NAME', 'Freeform') settings max_memory_usage = 20000000, schema_inference_use_cache_for_file = 0, input_format_freeform_max_search_steps = 0" 2>&1 | grep -oE 'BAD_ARGUMENTS|MEMORY_LIMIT_EXCEEDED|TIMEOUT_EXCEEDED' | head -1

echo "An unbounded search stops at the time limit"
words_row 24
$CLICKHOUSE_CLIENT -q "desc file('$FILE_NAME', 'Freeform') settings max_memory_usage = 1000000000, schema_inference_use_cache_for_file = 0, input_format_freeform_max_search_steps = 0, max_execution_time = 0.1" 2>&1 | grep -oE 'BAD_ARGUMENTS|MEMORY_LIMIT_EXCEEDED|TIMEOUT_EXCEEDED' | head -1

echo "An unbounded search for reading rows stops at the time limit"
STRUCTURE=$(seq -f 'c%g String' 0 23 | paste -sd, -)
$CLICKHOUSE_CLIENT -q "select count() from file('$FILE_NAME', 'Freeform', '$STRUCTURE') settings max_memory_usage = 1000000000, input_format_freeform_max_search_steps = 0, max_execution_time = 0.1" 2>&1 | grep -oE 'BAD_ARGUMENTS|MEMORY_LIMIT_EXCEEDED|TIMEOUT_EXCEEDED' | head -1

echo "An unbounded search stops at KILL QUERY"
QUERY_ID="${CLICKHOUSE_TEST_UNIQUE_NAME}_kill"
$CLICKHOUSE_CLIENT --query_id "$QUERY_ID" -q "desc file('$FILE_NAME', 'Freeform') settings max_memory_usage = 2000000000, schema_inference_use_cache_for_file = 0, input_format_freeform_max_search_steps = 0" 2>&1 | grep -oE 'BAD_ARGUMENTS|MEMORY_LIMIT_EXCEEDED|TIMEOUT_EXCEEDED|QUERY_WAS_CANCELLED' | head -1 &
# Wait until the search is under way, so the cancellation has to be seen inside it.
for _ in $(seq 1 600); do
    [ "$($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.processes WHERE query_id = '$QUERY_ID' AND memory_usage > 10000000")" = "1" ] && break
    sleep 0.1
done
timeout 10 $CLICKHOUSE_CLIENT -q "KILL QUERY WHERE query_id = '$QUERY_ID' SYNC FORMAT Null"
wait

echo "A long validation of the candidates stops at the time limit"
# A short first row keeps the search within the default bound, and long later rows make every candidate's validation
# expensive. The last checked row fails every candidate, so only the time limit can stop the query early.
word=$(head -c 16384 /dev/zero | tr '\0' a)
long_row=$(yes "$word" | head -n 10 | paste -sd $'\t' -)
{
    printf 'a\tb\tc\td\te\tf\tg\th\ti\tj\n'
    for _ in $(seq 1 98); do echo "$long_row"; done
    printf '%s\tx,y\n' "$long_row"
} > "$DATA_FILE"
$CLICKHOUSE_CLIENT -q "desc file('$FILE_NAME', 'Freeform') settings max_memory_usage = 150000000, schema_inference_use_cache_for_file = 0, max_execution_time = 0.1" 2>&1 | grep -oE 'BAD_ARGUMENTS|MEMORY_LIMIT_EXCEEDED|TIMEOUT_EXCEEDED' | head -1
