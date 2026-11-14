#!/usr/bin/env bash

# An unbounded search for the structure of a `Freeform` row (`input_format_freeform_max_search_steps = 0`) stops at
# the memory limit, the time limit and `KILL QUERY`, and so does a long validation of the candidates.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FILE_NAME=${CLICKHOUSE_TEST_UNIQUE_NAME}.data
DATA_FILE=${USER_FILES_PATH:?}/$FILE_NAME
KILL_OUTPUT=${CLICKHOUSE_TMP:?}/${CLICKHOUSE_TEST_UNIQUE_NAME}_kill.out

trap 'rm -f "$DATA_FILE" "$KILL_OUTPUT"' EXIT

# Writes two rows of N one-letter words separated by tabs: the number of candidate structures is exponential in N.
function words_row()
{
    local row
    row=$(seq "$1" | sed 's/.*/a/' | paste -sd $'\t' -)
    for _ in 1 2; do echo "$row"; done > "$DATA_FILE"
}

echo "An unbounded search stops at the memory limit"
# Every candidate of a wide row has many columns, so the limit is reached after a few steps of the search.
words_row 240
$CLICKHOUSE_CLIENT -q "desc file('$FILE_NAME', 'Freeform') settings max_memory_usage = 20000000, schema_inference_use_cache_for_file = 0, input_format_freeform_max_search_steps = 0" 2>&1 | grep -oE 'BAD_ARGUMENTS|MEMORY_LIMIT_EXCEEDED|TIMEOUT_EXCEEDED' | head -1

echo "An unbounded search stops at the time limit"
words_row 24
# A search has no partial structure to return, so `timeout_overflow_mode = 'break'` ends it with an error too.
$CLICKHOUSE_CLIENT -q "desc file('$FILE_NAME', 'Freeform') settings max_memory_usage = 1000000000, schema_inference_use_cache_for_file = 0, input_format_freeform_max_search_steps = 0, max_execution_time = 0.1, timeout_overflow_mode = 'break'" 2>&1 | grep -oE 'BAD_ARGUMENTS|MEMORY_LIMIT_EXCEEDED|TIMEOUT_EXCEEDED' | head -1

echo "An unbounded search for reading rows stops at the time limit"
STRUCTURE=$(seq -f 'c%g String' 0 23 | paste -sd, -)
$CLICKHOUSE_CLIENT -q "select count() from file('$FILE_NAME', 'Freeform', '$STRUCTURE') settings max_memory_usage = 1000000000, input_format_freeform_max_search_steps = 0, max_execution_time = 0.1" 2>&1 | grep -oE 'BAD_ARGUMENTS|MEMORY_LIMIT_EXCEEDED|TIMEOUT_EXCEEDED' | head -1

echo "An unbounded search stops at KILL QUERY"
# The KILL is sent only while the search is under way and far below its memory limit, so the search has to see it. If the
# query reaches the limit before any KILL could be sent (a stalled host), the attempt tells nothing and is repeated.
for attempt in 1 2 3; do
    QUERY_ID="${CLICKHOUSE_TEST_UNIQUE_NAME}_kill_${attempt}"
    timeout 60 $CLICKHOUSE_CLIENT --query_id "$QUERY_ID" -q "desc file('$FILE_NAME', 'Freeform') settings max_memory_usage = 2000000000, schema_inference_use_cache_for_file = 0, input_format_freeform_max_search_steps = 0" > "$KILL_OUTPUT" 2>&1 &
    client_pid=$!
    killed=""
    while [ -z "$killed" ] && kill -0 "$client_pid" 2>/dev/null && clickhouse_test_time_left; do
        killed=$($CLICKHOUSE_CLIENT -q "KILL QUERY WHERE query_id = '$QUERY_ID' AND peak_memory_usage BETWEEN 10000000 AND 1000000000 ASYNC SETTINGS kill_throw_if_noop = 0")
        [ -n "$killed" ] || sleep 0.1
    done
    wait "$client_pid"
    [ -n "$killed" ] && break
done
grep -oE 'BAD_ARGUMENTS|MEMORY_LIMIT_EXCEEDED|TIMEOUT_EXCEEDED|QUERY_WAS_CANCELLED' "$KILL_OUTPUT" | head -1

echo "A long validation of the candidates stops at the time limit"
# A short first row keeps the search within the default bound, and long later rows make every candidate's validation
# expensive. The last checked row fails every candidate, so only the time limit can stop the query early.
word=$(head -c 16384 /dev/zero | tr '\0' a)
long_row=$(for _ in $(seq 1 10); do echo "$word"; done | paste -sd $'\t' -)
{
    printf 'a\tb\tc\td\te\tf\tg\th\ti\tj\n'
    for _ in $(seq 1 98); do echo "$long_row"; done
    printf '%s\tx,y\n' "$long_row"
} > "$DATA_FILE"
$CLICKHOUSE_CLIENT -q "desc file('$FILE_NAME', 'Freeform') settings max_memory_usage = 150000000, schema_inference_use_cache_for_file = 0, max_execution_time = 0.1" 2>&1 | grep -oE 'BAD_ARGUMENTS|MEMORY_LIMIT_EXCEEDED|TIMEOUT_EXCEEDED' | head -1
