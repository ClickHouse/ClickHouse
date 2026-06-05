#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Tests native Arrow IPC body compression (LZ4_FRAME and ZSTD) for the ArrowStream format. For each
# codec the data must round-trip through every writer/reader combination (native and Apache Arrow
# library), all matching the original.

GEN="SELECT toInt32(number) AS i, toString(number) AS s, number / 3 AS f, range(number % 3) AS arr,
            if(number % 4 = 0, NULL, number)::Nullable(UInt64) AS n,
            toLowCardinality(toString(number % 3)) AS lc
     FROM numbers(20)"

DATA_FILE="${CLICKHOUSE_TMP}/04318.arrows"

for codec in lz4_frame zstd; do
    for combo in "1 1" "1 0" "0 1"; do
        # shellcheck disable=SC2086
        set -- $combo
        WRITER=$1
        READER=$2
        ${CLICKHOUSE_LOCAL} --query "
            INSERT INTO FUNCTION file('${DATA_FILE}', 'ArrowStream') ${GEN}
            SETTINGS output_format_arrow_use_native_writer = ${WRITER},
                     output_format_arrow_compression_method = '${codec}',
                     output_format_arrow_string_as_string = 1,
                     engine_file_truncate_on_insert = 1,
                     max_block_size = 7"
        RESULT=$(${CLICKHOUSE_LOCAL} --query "SELECT * FROM file('${DATA_FILE}', 'ArrowStream') ORDER BY i SETTINGS input_format_arrow_use_native_reader = ${READER}")
        REFERENCE=$(${CLICKHOUSE_LOCAL} --query "${GEN} ORDER BY i")
        if [ "$RESULT" = "$REFERENCE" ]; then
            echo "${codec} writer=${WRITER} reader=${READER}: OK"
        else
            echo "${codec} writer=${WRITER} reader=${READER}: MISMATCH"
        fi
    done
done

rm -f "${DATA_FILE}"
