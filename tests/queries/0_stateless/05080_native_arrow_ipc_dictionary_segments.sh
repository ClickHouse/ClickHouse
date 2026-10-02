#!/usr/bin/env bash
# Tags: no-fasttest
# This test requires PyArrow to write dictionary deltas.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
set -e

TMP_DIR="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
mkdir -p "$TMP_DIR"
trap 'rm -rf "$TMP_DIR"' EXIT

python3 - "$TMP_DIR" <<'PY'
import json
import os
import shlex
import subprocess
import sys
import pyarrow as pa
import pyarrow.ipc as ipc

out = sys.argv[1]
list_type = pa.list_(pa.null(), 2)
struct_type = pa.struct([pa.field("a", pa.null()), pa.field("b", list_type, nullable=False)])
struct_value = {"a": None, "b": [None, None]}
local = shlex.split(os.environ["CLICKHOUSE_LOCAL"])
queries = []
checks = []

# All-valid bufferless deltas use constants. Deltas with outer nulls use ordinary columns, and adjacent
# ordinary deltas merge. Entry validity must survive each transition independently of the target type.
for fmt, writer_type in (("Arrow", ipc.new_file), ("ArrowStream", ipc.new_stream)):
    for shape, value_type, value, ch_type, default in (
        ("array", list_type, [None, None], "Array(Nullable(UInt8))", []),
        ("tuple", struct_type, struct_value, "Tuple(a Nullable(UInt8), b Array(Nullable(UInt8)))",
         {"a": None, "b": []}),
        ("nullable_tuple", struct_type, struct_value,
         "Nullable(Tuple(a Nullable(UInt8), b Array(Nullable(UInt8))))", None),
    ):
        for nullable in (False, True):
            if shape == "nullable_tuple" and not nullable:
                continue
            schema = pa.schema([pa.field("c", pa.dictionary(pa.int16(), value_type), nullable=nullable)])
            entries = [None, value]
            stages = [(list(entries), [1, 1, 1])]
            for addition in ([value, value], [None, value], [None, value], [value, value]):
                entries.extend(addition)
                indices = [len(entries) - 1, 1, len(entries) - 2, 2, 1]
                if nullable:
                    indices[-1] = None
                else:
                    indices = [i if entries[i] is not None else 1 for i in indices]
                stages.append((list(entries), indices))

            path = f"{out}/{shape}_{nullable}.{fmt}"
            expected = []
            with writer_type(path, schema, options=ipc.IpcWriteOptions(emit_dictionary_deltas=True)) as writer:
                for values, indices in stages:
                    column = pa.DictionaryArray.from_arrays(
                        pa.array(indices, type=pa.int16()), pa.array(values, type=value_type))
                    writer.write_batch(pa.record_batch([column], schema=schema))
                    for index in indices:
                        item = None if index is None else values[index]
                        expected.append(default if item is None else item)

            queries.append(
                f"SELECT ifNull(toJSONString(c), 'null') FROM file('{path}', '{fmt}', 'c {ch_type}') "
                f"FORMAT TSVRaw SETTINGS allow_experimental_nullable_tuple_type={int(shape == 'nullable_tuple')};")
            checks.append((f"OK {fmt} {shape} {'nullable' if nullable else 'non-nullable'}", expected))

result = subprocess.run(local + [
    "--path", f"{out}/local", "--max_threads=1", "--max_block_size=2",
    "--multiquery", "--query", "\n".join(queries),
], text=True, capture_output=True)
assert result.returncode == 0 and not result.stderr, (result.returncode, result.stderr)
lines = result.stdout.splitlines()
position = 0
for name, expected in checks:
    actual = [json.loads(line) for line in lines[position:position + len(expected)]]
    assert actual == expected, (name, actual, expected)
    position += len(expected)
    print(name)
assert position == len(lines), lines[position:]
PY
