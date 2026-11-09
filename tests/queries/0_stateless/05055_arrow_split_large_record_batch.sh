#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Arrow offsets its String/Binary/List buffers with 32-bit integers, so a block holding more than that has
# to be written as several record batches. `output_format_arrow_max_record_batch_size_bytes` lowers the
# bound, which exercises the same splitting with kilobytes instead of the 2 GiB a real overflow needs.
#
# Compression is off because older pyarrow cannot read the "uncompressed" sentinel.

FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_split.arrows"
trap 'rm -f "$FILE"' EXIT

batches() {
    python3 - "$FILE" <<'PY'
import sys
import pyarrow as pa
import pyarrow.ipc as ipc

with pa.OSFile(sys.argv[1], "rb") as src:
    print(sum(1 for _ in ipc.open_stream(src)))
PY
}

run() {
    local label="$1" expr="$2" limit="$3" extra="${4:-}"
    $CLICKHOUSE_LOCAL --allow_suspicious_fixed_string_types 1 --query "
        SELECT number AS n, ${expr} AS v
        FROM numbers(64)
        SETTINGS max_block_size = 64,
                 output_format_arrow_compression_method = 'none',
                 output_format_arrow_max_record_batch_size_bytes = ${limit}${extra}
        FORMAT ArrowStream
    " > "$FILE"
    echo -n "${label}: batches>1=$([ "$(batches)" -gt 1 ] && echo 1 || echo 0) "
    $CLICKHOUSE_LOCAL --query "SELECT count(), sum(n), uniqExact(v) FROM file('$FILE', ArrowStream)"
}

# One block of 64 rows against a small bound, so each shape must split.
run "String"             "repeat('x', 64)"                                            512
run "Nullable(String)"   "if(number % 3 = 0, NULL, repeat('y', 64))::Nullable(String)" 512
run "LowCardinality"     "toLowCardinality(repeat(toString(number % 4), 64))"          512
run "Array(String)"      "[repeat('z', 64)]"                                          512
run "Map"                "map('k', repeat('m', 64))"                                  512
run "Tuple"              "(repeat('p', 64), repeat('q', 64))"                         512
run "Variant"            "if(number % 2, number, repeat('v', 64))::Variant(UInt64, String)" 512
# Only the offsets mode gives `FixedString` a buffer the bound applies to; the default maps it to a
# fixed-width Arrow type instead.
run "FixedString" "toFixedString(repeat('f', 64), 64)" 512 ", output_format_arrow_fixed_string_as_fixed_byte_array = 0"

# A fixed-width column has no offsets buffer to overflow, so the bound leaves it in one batch.
run "FixedString fixed-width" "toFixedString(repeat('f', 64), 64)" 512
run "UInt64"             "number"                                                     64
