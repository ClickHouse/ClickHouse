#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: needs the pyarrow Python module to build the Arrow streams.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TMP_DIR="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
mkdir -p "$TMP_DIR"
trap 'rm -rf "$TMP_DIR"' EXIT

# A delta dictionary batch appends values to a dictionary decoded earlier in the stream, so its values must
# decode to the same column layout as the ones already registered. Under a raw-byte requested type (`Int128`
# here) a variable-width `binary` dictionary is reinterpreted only when every value has the type's width,
# which is decided per batch: a base holding 16-byte values decodes to `Int128`, a delta adding a 3-byte
# value stays `String`. Such a dictionary has no reading as `Int128` as a whole, just as an inline `binary`
# column mixing widths fails in the cast, so the delta is rejected instead of being appended to a column of
# another layout. Read as `String`, or when the delta keeps the width, the delta merges as usual.

python3 - "$TMP_DIR" <<'PYEOF'
import sys
import pyarrow as pa
import pyarrow.ipc as ipc

out = sys.argv[1]


def raw(n):
    return n.to_bytes(16, "little", signed=True)


# `emit_dictionary_deltas` makes pyarrow write the second batch's extended dictionary as a delta batch holding
# only the added value, instead of a replacement dictionary.
options = ipc.IpcWriteOptions(emit_dictionary_deltas=True)


def write(name, base_values, extended_values):
    first = pa.DictionaryArray.from_arrays(pa.array([0, 1, 0], type=pa.int32()), base_values)
    second = pa.DictionaryArray.from_arrays(pa.array([2, 1, 0], type=pa.int32()), extended_values)
    schema = pa.schema([pa.field("b", first.type)])
    path = f"{out}/{name}.arrows"
    with ipc.new_stream(path, schema, options=options) as writer:
        writer.write_batch(pa.record_batch([first], schema=schema))
        writer.write_batch(pa.record_batch([second], schema=schema))
    with open(path, "rb") as f:
        reader = ipc.MessageReader.open_stream(f)
        kinds = [message.type for message in iter(reader.read_next_message, None)]
    assert kinds == ["schema", "dictionary", "record batch", "dictionary", "record batch"], kinds


base = [raw(12345), raw(-1)]
write("width_changes", pa.array(base, type=pa.binary()), pa.array(base + [b"abc"], type=pa.binary()))
write("width_kept", pa.array(base, type=pa.binary()), pa.array(base + [raw(67890)], type=pa.binary()))
PYEOF

# The two record batches may be decoded in parallel, so the rows are sorted for a stable output.
read_as()
{
    ${CLICKHOUSE_LOCAL} --allow_suspicious_low_cardinality_types=1 \
        --query "SELECT $2 FROM file('${TMP_DIR}/$1.arrows', 'ArrowStream', '$3') ORDER BY b" 2>&1
}

echo "--- width changes, b String ---"
read_as width_changes "hex(b)" "b String"
echo "--- width changes, b LowCardinality(String) ---"
read_as width_changes "hex(b)" "b LowCardinality(String)"
echo "--- width changes, b Int128: rejected ---"
read_as width_changes "b" "b Int128" | grep -o "TYPE_MISMATCH" | head -n 1
echo "--- width kept, b Int128 ---"
read_as width_kept "b" "b Int128"
echo "--- width kept, b LowCardinality(Int128) ---"
read_as width_kept "b" "b LowCardinality(Int128)"
