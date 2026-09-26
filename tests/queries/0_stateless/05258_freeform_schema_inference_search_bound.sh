#!/usr/bin/env bash

# The search for the structure of a `Freeform` row branches on every field that several matchers read
# alike, so a wide row of strings has exponentially many candidate structures. The search is bounded,
# and a row that exceeds the bound is refused with an error instead of exhausting memory.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FILE_NAME=${CLICKHOUSE_TEST_UNIQUE_NAME}.data
DATA_FILE=${USER_FILES_PATH:?}/$FILE_NAME

trap 'rm -f "$DATA_FILE"' EXIT

# The memory limit makes the unbounded search fail quickly instead of taking the whole server memory.
SETTINGS="max_memory_usage = 150000000, schema_inference_use_cache_for_file = 0"

echo "A wide row of numbers is inferred"
for _ in 1 2; do seq -s $'\t' 1 24; done > "$DATA_FILE"
$CLICKHOUSE_CLIENT -q "desc file('$FILE_NAME', 'Freeform') settings $SETTINGS" | grep -c Int64

echo "A narrow row of strings is inferred"
for _ in 1 2; do printf 'a\tb\tc\td\te\tf\tg\th\n'; done > "$DATA_FILE"
$CLICKHOUSE_CLIENT -q "desc file('$FILE_NAME', 'Freeform') settings $SETTINGS" | grep -c String

echo "A wide row of tab-separated strings is refused"
for _ in 1 2; do printf 'a\tb\tc\td\te\tf\tg\th\ti\tj\tk\tl\tm\tn\to\tp\tq\tr\ts\tt\tu\tv\tw\tx\n'; done > "$DATA_FILE"
$CLICKHOUSE_CLIENT -q "desc file('$FILE_NAME', 'Freeform') settings $SETTINGS" 2>&1 | grep -oF -e 'BAD_ARGUMENTS' -e 'MEMORY_LIMIT_EXCEEDED' | head -1

echo "A wide row of quoted strings is refused"
for _ in 1 2; do for i in $(seq 1 24); do printf "'s%d' " "$i"; done; echo; done > "$DATA_FILE"
$CLICKHOUSE_CLIENT -q "desc file('$FILE_NAME', 'Freeform') settings $SETTINGS" 2>&1 | grep -oF -e 'BAD_ARGUMENTS' -e 'MEMORY_LIMIT_EXCEEDED' | head -1
