#!/usr/bin/env bash

# The search for the structure of a `Freeform` row branches on every field that several matchers read
# alike, so a wide row of strings has exponentially many candidate structures. The search is bounded by
# `input_format_freeform_max_search_steps`, and a row that exceeds the bound is refused with an error
# instead of exhausting memory.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FILE_NAME=${CLICKHOUSE_TEST_UNIQUE_NAME}.data
DATA_FILE=${USER_FILES_PATH:?}/$FILE_NAME

trap 'rm -f "$DATA_FILE"' EXIT

# The memory limit makes an unbounded search fail quickly with `MEMORY_LIMIT_EXCEEDED` (which fails the test)
# instead of taking the whole server memory.
SETTINGS="max_memory_usage = 150000000, schema_inference_use_cache_for_file = 0"

# Writes two rows of the first N letters separated by tabs.
function strings_row()
{
    local row
    row=$(printf '%s\t' a b c d e f g h i j k l m n o p q r s t u v w x | cut -f "1-$1")
    for _ in 1 2; do echo "$row"; done > "$DATA_FILE"
}

function describe()
{
    $CLICKHOUSE_CLIENT -q "desc file('$FILE_NAME', 'Freeform') settings $SETTINGS $1" 2>&1
}

echo "A wide row of numbers is inferred"
for _ in 1 2; do seq -s $'\t' 1 24; done > "$DATA_FILE"
describe | grep -c Int64

echo "The widest row of tab-separated strings within the default bound is inferred"
strings_row 10
describe | grep -c String

echo "The narrowest row of tab-separated strings beyond the default bound is refused"
strings_row 11
describe | grep -oE 'BAD_ARGUMENTS|MEMORY_LIMIT_EXCEEDED' | head -1

echo "It is inferred when the bound is raised or removed"
describe ", input_format_freeform_max_search_steps = 100000" | grep -c String
describe ", input_format_freeform_max_search_steps = 0" | grep -c String
describe ", compatibility = '26.9'" | grep -c String

echo "A schema cached under a raised bound is not reused under the default bound"
CACHE_SETTINGS="max_memory_usage = 150000000, schema_inference_use_cache_for_file = 1"
$CLICKHOUSE_CLIENT -q "desc file('$FILE_NAME', 'Freeform') settings $CACHE_SETTINGS, input_format_freeform_max_search_steps = 100000" 2>&1 | grep -c String
$CLICKHOUSE_CLIENT -q "desc file('$FILE_NAME', 'Freeform') settings $CACHE_SETTINGS" 2>&1 | grep -oE 'BAD_ARGUMENTS|MEMORY_LIMIT_EXCEEDED' | head -1

echo "A wide row of tab-separated strings is refused"
strings_row 24
describe | grep -oE 'BAD_ARGUMENTS|MEMORY_LIMIT_EXCEEDED' | head -1

echo "An unbounded search stops at the memory limit"
strings_row 24
$CLICKHOUSE_CLIENT -q "desc file('$FILE_NAME', 'Freeform') settings max_memory_usage = 50000000, schema_inference_use_cache_for_file = 0, input_format_freeform_max_search_steps = 0" 2>&1 | grep -oE 'BAD_ARGUMENTS|MEMORY_LIMIT_EXCEEDED|TIMEOUT_EXCEEDED' | head -1

echo "An unbounded search stops at the time limit"
$CLICKHOUSE_CLIENT -q "desc file('$FILE_NAME', 'Freeform') settings max_memory_usage = 1000000000, schema_inference_use_cache_for_file = 0, input_format_freeform_max_search_steps = 0, max_execution_time = 0.1" 2>&1 | grep -oE 'BAD_ARGUMENTS|MEMORY_LIMIT_EXCEEDED|TIMEOUT_EXCEEDED' | head -1

echo "An unbounded search for reading rows stops at the time limit"
STRUCTURE=$(seq -f 'c%g String' 0 23 | paste -sd, -)
$CLICKHOUSE_CLIENT -q "select count() from file('$FILE_NAME', 'Freeform', '$STRUCTURE') settings max_memory_usage = 1000000000, input_format_freeform_max_search_steps = 0, max_execution_time = 0.1" 2>&1 | grep -oE 'BAD_ARGUMENTS|MEMORY_LIMIT_EXCEEDED|TIMEOUT_EXCEEDED' | head -1

echo "A wide row of quoted strings is refused"
for _ in 1 2; do for i in $(seq 1 24); do printf "'s%d' " "$i"; done; echo; done > "$DATA_FILE"
describe | grep -oE 'BAD_ARGUMENTS|MEMORY_LIMIT_EXCEEDED' | head -1
