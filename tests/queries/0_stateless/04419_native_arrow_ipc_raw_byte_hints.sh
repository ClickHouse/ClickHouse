#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: needs the pyarrow Python module to build the Arrow IPC stream.
#
# Regression test for the native Arrow IPC reader's raw-byte type-hint conversions. The library reader
# reinterprets fixed_size_binary AND variable binary as UUID/IPv6/big-integer, recursively through nested
# types. The native reader previously only converted top-level fixed_size_binary, so:
#   - a variable Binary column of 16/32 raw bytes could not be read as IPv6/Int128/... (gap 1), and
#   - nested shapes like Array(Int128) / Tuple(ip IPv6) were decoded as FixedString/String and text-cast (gap 2).
# Each case is read with the native reader and compared against the library reader (the parity oracle); the
# decoded value is also printed so the reference pins the correct result.

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

def write(suffix, field, array):
    schema = pa.schema([field])
    w = writer_factory(f"{prefix}_{suffix}.arrow", schema)
    w.write_batch(pa.record_batch([array], schema=schema))
    w.close()

b16a = bytes(range(16))
b16b = bytes(range(16, 32))
i128 = (12345).to_bytes(16, 'little')
i128b = (67890).to_bytes(16, 'little')

# gap 1: variable Binary -> IPv6 and -> Int128 (top level)
write('bin_ipv6', pa.field('x', pa.binary()), pa.array([b16a, b16b], type=pa.binary()))
write('bin_i128', pa.field('x', pa.binary()), pa.array([i128, i128b], type=pa.binary()))
# gap 2: nested list<fixed_size_binary(16)> -> Array(Int128), struct<ip: binary> -> Tuple(ip IPv6)
write('arr_i128', pa.field('x', pa.list_(pa.binary(16))), pa.array([[i128, i128b]], type=pa.list_(pa.binary(16))))
write('tup_ipv6', pa.field('x', pa.struct([('ip', pa.binary())])),
      pa.array([{'ip': b16a}], type=pa.struct([('ip', pa.binary())])))
EOF

    both() { # $1=file-suffix $2=hint $3=select-expr : print native result, then native-vs-library parity
        local n l
        n=$(${CLICKHOUSE_LOCAL} --input_format_arrow_use_native_reader=1 \
            --query "SELECT $3 FROM file('${PREFIX}_$1.arrow', '${FORMAT}', 'x $2')" 2>&1)
        l=$(${CLICKHOUSE_LOCAL} --input_format_arrow_use_native_reader=0 \
            --query "SELECT $3 FROM file('${PREFIX}_$1.arrow', '${FORMAT}', 'x $2')" 2>&1)
        echo "$n"
        [ "$n" = "$l" ] && echo "native==library" || echo "MISMATCH native=[$n] library=[$l]"
    }

    echo "--- ${FORMAT}: Binary -> IPv6 ---";        both bin_ipv6 'IPv6'              'hex(x)'
    echo "--- ${FORMAT}: Binary -> Int128 ---";      both bin_i128 'Int128'            'x'
    echo "--- ${FORMAT}: Array(Int128) nested ---";  both arr_i128 'Array(Int128)'     'x'
    echo "--- ${FORMAT}: Tuple(ip IPv6) nested ---"; both tup_ipv6 'Tuple(ip IPv6)'    'hex(x.ip)'

    rm -f "${PREFIX}_bin_ipv6.arrow" "${PREFIX}_bin_i128.arrow" "${PREFIX}_arr_i128.arrow" "${PREFIX}_tup_ipv6.arrow"
done
