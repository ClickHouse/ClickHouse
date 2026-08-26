#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: needs the pyarrow Python module to build the Arrow IPC stream.
#
# Regression test for the native Arrow IPC reader: a fixed_size_binary column read with a big-integer
# (Int128/Int256) type hint must be rejected unless its byte width equals the integer width. Otherwise the
# wrong-width value is left as FixedString(N) and the later castColumn parses the raw bytes as text (e.g. a
# fixed_size_binary(1) holding ASCII '1' decoded as Int128(1)), accepting malformed data that the Apache
# Arrow library path rejects. A correct-width value must still decode by reinterpreting its little-endian
# bytes.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

for FORMAT in ArrowStream Arrow; do
    PREFIX="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_${FORMAT}"

    python3 - "$PREFIX" "$FORMAT" <<'EOF'
import sys
import pyarrow as pa
import pyarrow.ipc as ipc

prefix, fmt = sys.argv[1], sys.argv[2]
writer_factory = ipc.new_stream if fmt == 'ArrowStream' else ipc.new_file

def write(suffix, byte_width, values):
    arr = pa.array(values, type=pa.binary(byte_width))
    schema = pa.schema([pa.field('x', pa.binary(byte_width))])
    w = writer_factory(f"{prefix}_{suffix}.arrow", schema)
    w.write_batch(pa.record_batch([arr], schema=schema))
    w.close()

# Wrong width: a single ASCII byte that would text-parse to a small integer.
write('wrong', 1, [b'1', b'2'])
# Correct width: 12345 as little-endian bytes for the 16- and 32-byte integers.
write('ok16', 16, [(12345).to_bytes(16, 'little')])
write('ok32', 32, [(12345).to_bytes(32, 'little')])
EOF

    reject_check() { # $1=label $2=file-suffix $3=hint
        if ${CLICKHOUSE_LOCAL} --input_format_arrow_use_native_reader=1 \
            --query "SELECT x FROM file('${PREFIX}_$2.arrow', '${FORMAT}', 'x $3') FORMAT Null" >/dev/null 2>&1
        then
            echo "${1}: ACCEPTED (BUG)"
        else
            echo "${1}: rejected"
        fi
    }

    echo "--- ${FORMAT} ---"
    reject_check "wrong-width Int128" wrong Int128
    reject_check "wrong-width Int256" wrong Int256
    echo -n "correct-width Int128: "
    ${CLICKHOUSE_LOCAL} --input_format_arrow_use_native_reader=1 \
        --query "SELECT x FROM file('${PREFIX}_ok16.arrow', '${FORMAT}', 'x Int128')"
    echo -n "correct-width Int256: "
    ${CLICKHOUSE_LOCAL} --input_format_arrow_use_native_reader=1 \
        --query "SELECT x FROM file('${PREFIX}_ok32.arrow', '${FORMAT}', 'x Int256')"

    rm -f "${PREFIX}_wrong.arrow" "${PREFIX}_ok16.arrow" "${PREFIX}_ok32.arrow"
done
