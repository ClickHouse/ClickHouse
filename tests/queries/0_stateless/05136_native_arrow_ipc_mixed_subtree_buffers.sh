#!/usr/bin/env bash
# Tags: long, no-fasttest
# This test requires PyArrow to write nested arrays with different physical buffer layouts.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
set -e

TMP_DIR="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
mkdir -p "$TMP_DIR"
trap 'rm -rf "$TMP_DIR"' EXIT

python3 - "$TMP_DIR" <<'PY'
from decimal import Decimal
from pathlib import Path
import json
import os
import shlex
import subprocess
import sys

import pyarrow as pa
import pyarrow.ipc as ipc

out = Path(sys.argv[1])
local = shlex.split(os.environ["CLICKHOUSE_LOCAL"]) + [
    "--path", str(out / "local"), "--max_threads=1", "--allow_experimental_nullable_tuple_type=1",
    "--output_format_json_named_tuples_as_objects=1", "--output_format_json_quote_64bit_integers=0",
    "--output_format_json_quote_decimals=0", "--session_timezone=UTC",
]


def mixed_struct(values, mask=None):
    return pa.StructArray.from_arrays([pa.nulls(len(values)), values], names=["n", "v"], mask=mask)


def mixed_struct_rows(values):
    return [{"n": None, "v": value} for value in values]


columns = {}
expected = {}
targets = {}


def add_mixed_column(name, values, decoded, target):
    columns[name] = mixed_struct(values)
    expected[name] = mixed_struct_rows(decoded)
    targets[name] = f"Nullable(Tuple(n Nullable(UInt8), v {target}))"


# A bufferless first child precedes each supported primitive and variable-width buffer layout.
for signed in (True, False):
    for bits in (8, 16, 32, 64):
        name = f"{'int' if signed else 'uint'}{bits}"
        add_mixed_column(name, pa.array([1, 2, 3], type=getattr(pa, name)()), [1, 2, 3], f"{'Int' if signed else 'UInt'}{bits}")
for bits in (16, 32, 64):
    add_mixed_column(f"float{bits}", pa.array([1.5, 2.5, 3.5], type=getattr(pa, f"float{bits}")()),
        [1.5, 2.5, 3.5], "Float64" if bits == 64 else "Float32")
add_mixed_column("bool", pa.array([True, False, True]), [True, False, True], "Bool")
for bits, precision in ((32, 9), (64, 18), (128, 38), (256, 76)):
    data_type = getattr(pa, f"decimal{bits}")(precision, 2)
    add_mixed_column(f"decimal{bits}", pa.array([Decimal("1.25"), Decimal("2.50"), Decimal("3.75")], data_type),
        [1.25, 2.5, 3.75], f"Decimal({precision}, 2)")
for name, data_type in (("string", pa.string()), ("large_string", pa.large_string()),
                        ("string_view", pa.string_view()), ("binary", pa.binary()),
                        ("large_binary", pa.large_binary()), ("binary_view", pa.binary_view())):
    add_mixed_column(name, pa.array(["a", "a value longer than an inline view", "c"], data_type),
        ["a", "a value longer than an inline view", "c"], "String")
add_mixed_column("fixed_binary", pa.array([b"abcd", b"efgh", b"ijkl"], pa.binary(4)), ["abcd", "efgh", "ijkl"], "FixedString(4)")
add_mixed_column("date32", pa.array([0, 1, 2], pa.date32()), ["1970-01-01", "1970-01-02", "1970-01-03"], "Date32")
add_mixed_column("date64", pa.array([0, 86400000, 172800000], pa.date64()),
    ["1970-01-01 00:00:00", "1970-01-02 00:00:00", "1970-01-03 00:00:00"], "DateTime")
add_mixed_column("timestamp", pa.array([0, 1000, 2000], pa.timestamp("ms", tz="UTC")),
    ["1970-01-01 00:00:00.000", "1970-01-01 00:00:01.000", "1970-01-01 00:00:02.000"], "DateTime64(3)")
add_mixed_column("time32", pa.array([0, 1000, 2000], pa.time32("ms")), ["00:00:00.000", "00:00:01.000", "00:00:02.000"], "Time64(3)")
add_mixed_column("time64", pa.array([0, 1000000, 2000000], pa.time64("us")),
    ["00:00:00.000000", "00:00:01.000000", "00:00:02.000000"], "Time64(6)")
add_mixed_column("duration", pa.array([1, 2, 3], pa.duration("us")), [1, 2, 3], "Int64")
add_mixed_column("nullable_value", pa.array([1, None, 3], pa.int32()), [1, None, 3], "Nullable(Int32)")
columns["nullable_struct"] = mixed_struct(pa.array([1, 2, 3], pa.int32()), pa.array([False, True, False]))
targets["nullable_struct"] = "Nullable(Tuple(n Nullable(UInt8), v Int32))"
expected["nullable_struct"] = [mixed_struct_rows([1])[0], None, mixed_struct_rows([3])[0]]

# Container children have their own node lengths, including the shorter children of dense unions.
values = mixed_struct(pa.array([11, 22, 33], pa.int32()))
decoded = mixed_struct_rows([11, 22, 33])
mixed_type = "Tuple(n Nullable(UInt8), v Int32)"
for name, array_type, offset_type in (("list", pa.ListArray, pa.int32()),
                                      ("large_list", pa.LargeListArray, pa.int64())):
    columns[name] = array_type.from_arrays(pa.array([0, 1, 1, 3], offset_type), values)
    expected[name] = [decoded[:1], [], decoded[1:]]
    targets[name] = f"Array({mixed_type})"
columns["map"] = pa.MapArray.from_arrays(pa.array([0, 1, 1, 3]), pa.array(["a", "b", "c"]), values)
targets["map"] = f"Map(String, {mixed_type})"
expected["map"] = [{"a": decoded[0]}, {}, {"b": decoded[1], "c": decoded[2]}]
columns["fixed_list"] = pa.FixedSizeListArray.from_arrays(mixed_struct(pa.array([1, 2, 3, 4, 5, 6], pa.int32())), 2)
targets["fixed_list"] = f"Array({mixed_type})"
expected["fixed_list"] = [mixed_struct_rows([1, 2]), mixed_struct_rows([3, 4]), mixed_struct_rows([5, 6])]
for signed in (True, False):
    for bits in (8, 16, 32, 64):
        name = f"dictionary_{'int' if signed else 'uint'}{bits}"
        data_type = getattr(pa, f"{'int' if signed else 'uint'}{bits}")()
        columns[name] = pa.DictionaryArray.from_arrays(pa.array([2, 0, 1], data_type), values)
        expected[name] = [decoded[2], decoded[0], decoded[1]]
        targets[name] = mixed_type
union_values = pa.StructArray.from_arrays(
    [pa.array([None, None, None], pa.int8()), pa.array([11, 22, 33], pa.int32())], names=["n", "v"])
columns["dense_union"] = pa.UnionArray.from_dense(
    pa.array([0, 1, 0], pa.int8()), pa.array([0, 0, 1], pa.int32()),
    [union_values.slice(0, 2), pa.array([44], pa.int16()), pa.nulls(1)],
    field_names=["mixed", "number", "null"])
expected["dense_union"] = [decoded[0], 44, decoded[1]]
columns["dense_union_retained"] = pa.UnionArray.from_dense(
    pa.array([0, 1, 0], pa.int8()), pa.array([0, 0, 1], pa.int32()),
    [union_values, pa.array([44], pa.int16()), pa.nulls(1)], field_names=["mixed", "number", "null"])
expected["dense_union_retained"] = expected["dense_union"]
columns["sparse_union"] = pa.UnionArray.from_sparse(
    pa.array([0, 1, 0], pa.int8()), [union_values, pa.array([44, 55, 66], pa.int16()), pa.nulls(3)],
    field_names=["mixed", "number", "null"])
expected["sparse_union"] = [decoded[0], 55, decoded[2]]
for name in ("dense_union", "dense_union_retained", "sparse_union"):
    targets[name] = "Variant(Tuple(n Nullable(Int8), v Nullable(Int32)), Int16)"

structure = ", ".join(f"`{name}` {targets[name]}" for name in columns)
batch = pa.record_batch(list(columns.values()), names=list(columns))
expected_rows = [{name: values[row] for name, values in expected.items()} for row in range(3)]
projection = pa.record_batch([
    pa.RunEndEncodedArray.from_arrays(pa.array([1, 3], pa.int16()), pa.array([7, 8], pa.int32())),
    pa.ListViewArray.from_arrays(pa.array([0, 1, 2]), pa.array([1, 1, 1]), pa.array([7, 8, 9])),
    pa.array([(0, 0, 0)] * 3, pa.month_day_nano_interval()),
    columns["int32"],
], names=["run_end", "list_view", "interval", "keep"])

queries = []
checks = []

for fmt, writer in (("Arrow", ipc.new_file), ("ArrowStream", ipc.new_stream)):
    for codec in (None, "lz4", "zstd"):
        name = codec or "uncompressed"
        paths = []
        for suffix, data in (("values", batch), ("empty", batch.slice(0, 0))):
            path = out / f"{fmt}_{name}_{suffix}"
            with writer(path, data.schema, options=ipc.IpcWriteOptions(compression=codec)) as stream:
                stream.write_batch(data)
            paths.append(path)
        queries.extend(
            f"SELECT * FROM file('{path}', '{fmt}', {{structure:String}}) FORMAT JSONEachRow;" for path in paths)
        checks.append((f"{fmt} {name}: OK", expected_rows))

    # Unsupported unrequested fields are traversed without validating or decoding their value buffers.
    path = out / f"{fmt}_projection"
    with writer(path, projection.schema) as stream:
        stream.write_batch(projection)
    queries.append(
        f"SELECT keep FROM file('{path}', '{fmt}', 'keep Tuple(n Nullable(UInt8), v Int32)') FORMAT JSONEachRow;")
    checks.append((f"{fmt} projection: OK", [{"keep": row} for row in expected["int32"]]))

result = subprocess.run(local + [
    f"--param_structure={structure}", "--multiquery", "--query", "\n".join(queries),
], text=True, capture_output=True)
assert result.returncode == 0 and not result.stderr, (result.returncode, result.stderr)
lines = result.stdout.splitlines()
position = 0
for name, expected_rows in checks:
    actual = [json.loads(line) for line in lines[position:position + len(expected_rows)]]
    assert actual == expected_rows, (name, actual, expected_rows)
    position += len(expected_rows)
    print(name)
assert position == len(lines), lines[position:]
PY
