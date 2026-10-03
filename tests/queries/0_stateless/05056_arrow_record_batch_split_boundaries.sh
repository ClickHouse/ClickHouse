#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The writer splits a chunk across record batches only when it exceeds the 32-bit offsets of the Arrow
# buffers, so a small result must give exactly one batch per block for every type shape. A wrong row
# count from the split logic shows up only here: the data reads back correctly either way.
#
# `FORMAT ArrowStream` into a file rather than `INSERT INTO FUNCTION file`, whose path squashes the
# blocks into one. Compression is off because older pyarrow cannot read the "uncompressed" sentinel.

FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_batches.arrows"
trap 'rm -f "$FILE"' EXIT

GEN="SELECT
    toString(number) AS s,
    if(number % 3 = 0, NULL, toString(number))::Nullable(String) AS ns,
    toLowCardinality(toString(number % 2)) AS lc,
    arrayMap(x -> toString(x), range(number % 3)) AS arr,
    arrayMap(y -> arrayMap(x -> toString(x), range(y)), range(number % 3)) AS arr2,
    map(toString(number), toString(number % 2)) AS m,
    (toString(number), toString(number % 2)) AS t,
    toFixedString(toString(number), 3) AS fs,
    number::Variant(UInt64, String) AS v,
    initializeAggregation('quantileState(0.5)', number) AS agg,
    toBFloat16(number) AS bf,
    number AS u
FROM numbers(6)"

for lc_as_dict in 0 1; do
    $CLICKHOUSE_LOCAL --query "${GEN} SETTINGS
        max_block_size = 2,
        output_format_arrow_compression_method = 'none',
        output_format_arrow_fixed_string_as_fixed_byte_array = 0,
        output_format_arrow_low_cardinality_as_dictionary = ${lc_as_dict}
        FORMAT ArrowStream" > "$FILE"

    echo -n "low_cardinality_as_dictionary=${lc_as_dict} record batches for 3 blocks: "
    python3 - "$FILE" <<'PY'
import sys
import pyarrow as pa
import pyarrow.ipc as ipc

with pa.OSFile(sys.argv[1], "rb") as src:
    print(sum(1 for _ in ipc.open_stream(src)))
PY

    $CLICKHOUSE_LOCAL --query "SELECT count(), sum(u) FROM file('$FILE', ArrowStream)"
done
