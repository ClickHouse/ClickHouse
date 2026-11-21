#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Tests native Arrow IPC body compression (LZ4_FRAME and ZSTD) for both the ArrowStream and Arrow (file)
# formats. For each codec the data must round-trip through every writer/reader combination (native and
# Apache Arrow library), and the natively-written compressed output must also be readable by a third-party
# Apache Arrow tool (pyarrow's `open_stream`/`open_file`), all matching the original.

GEN="SELECT toInt32(number) AS i, toString(number) AS s, number / 3 AS f, range(number % 3) AS arr,
            if(number % 4 = 0, NULL, number)::Nullable(UInt64) AS n,
            toLowCardinality(toString(number % 3)) AS lc
     FROM numbers(20)"

for format in ArrowStream Arrow; do
    DATA_FILE="${CLICKHOUSE_TMP}/04318_${format}.arrow"
    for codec in lz4_frame zstd; do
        for combo in "1 1" "1 0" "0 1"; do
            # shellcheck disable=SC2086
            set -- $combo
            WRITER=$1
            READER=$2
            ${CLICKHOUSE_LOCAL} --query "
                INSERT INTO FUNCTION file('${DATA_FILE}', '${format}') ${GEN}
                SETTINGS output_format_arrow_use_native_writer = ${WRITER},
                         output_format_arrow_compression_method = '${codec}',
                         output_format_arrow_string_as_string = 1,
                         engine_file_truncate_on_insert = 1,
                         max_block_size = 7"
            RESULT=$(${CLICKHOUSE_LOCAL} --query "SELECT * FROM file('${DATA_FILE}', '${format}') ORDER BY i SETTINGS input_format_arrow_use_native_reader = ${READER}")
            REFERENCE=$(${CLICKHOUSE_LOCAL} --query "${GEN} ORDER BY i")
            if [ "$RESULT" = "$REFERENCE" ]; then
                echo "${format} ${codec} writer=${WRITER} reader=${READER}: OK"
            else
                echo "${format} ${codec} writer=${WRITER} reader=${READER}: MISMATCH"
            fi
        done

        # The natively-written compressed output must be readable by Apache Arrow directly (footer/block
        # metadata and per-buffer compression must match the spec), not only by ClickHouse's readers.
        ${CLICKHOUSE_LOCAL} --query "
            INSERT INTO FUNCTION file('${DATA_FILE}', '${format}') ${GEN}
            SETTINGS output_format_arrow_use_native_writer = 1,
                     output_format_arrow_compression_method = '${codec}',
                     output_format_arrow_string_as_string = 1,
                     engine_file_truncate_on_insert = 1,
                     max_block_size = 7"
        python3 - "$DATA_FILE" "$format" "$codec" <<'EOF'
import sys
import pyarrow as pa
import pyarrow.ipc as ipc

path, fmt, codec = sys.argv[1], sys.argv[2], sys.argv[3]
with pa.OSFile(path, 'rb') as src:
    reader = ipc.open_file(src) if fmt == 'Arrow' else ipc.open_stream(src)
    table = reader.read_all()

ok = (
    table.num_rows == 20
    and table.column('i').to_pylist() == list(range(20))
    and table.column('s').to_pylist() == [str(x) for x in range(20)]
    and table.column('lc').to_pylist() == [str(x % 3) for x in range(20)]
)
print("%s %s pyarrow: %s" % (fmt, codec, "OK" if ok else "MISMATCH"))
EOF
    done
    rm -f "${DATA_FILE}"
done
