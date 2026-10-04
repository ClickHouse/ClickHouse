#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `ColumnBinary` is gated behind `allow_experimental_column_binary_format`.
CLICKHOUSE_CLIENT="${CLICKHOUSE_CLIENT} --allow_experimental_column_binary_format 1"

# `ColumnBinary` encodes a `ColumnConst` as a single value with `COL_IS_CONST`, which is the
# whole point of the format for a constant column: the frame stays small however many rows it
# covers. That only happens if the format itself reaches the pipeline, which asks the output
# format whether it wants materialized columns.
#
# `ColumnBinary` supports parallel formatting and `output_format_parallel_formatting` is on by
# default, so the usual `SELECT ... FORMAT ColumnBinary` goes through
# `ParallelFormattingOutputFormat`. The wrapper must answer the capability questions the same
# way the format it wraps does, otherwise the const column is expanded before it ever reaches
# the format and the frame grows by a factor of the row count.
# The frame is fetched over HTTP: on the native protocol the server sends `Native` blocks and
# the client does the formatting, so the server-side output format is not in the path at all.
mkdir -p "${USER_FILES_PATH}"

for p in 1 0; do
    frame="${USER_FILES_PATH}/05141_${CLICKHOUSE_DATABASE}_${p}.bin"
    ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&allow_experimental_column_binary_format=1&output_format_parallel_formatting=${p}" --data-binary \
        "SELECT materialize(number) AS n, 'a_long_constant_string_value' AS c
         FROM numbers(100000) FORMAT ColumnBinary" > "${frame}"
    eval "size_${p}=$(wc -c < "${frame}" | tr -d ' ')"
    # Both frames must still decode to the same values.
    ${CLICKHOUSE_CLIENT} --query \
        "SELECT sum(n), any(c), count() FROM file('${frame}', ColumnBinary, 'n UInt64, c String')"
    rm -f "${frame}"
done

echo "const preserved under parallel formatting: $(( size_1 == size_0 ))"
# Not merely equal, but actually small: the constant must not be repeated once per row.
echo "frame far smaller than the expanded column: $(( size_1 < 1000000 ))"
