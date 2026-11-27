#!/usr/bin/env bash
# Tags: long, no-fasttest
# This test requires PyArrow to write nullable structs in files and streams.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
set -e

TMP_DIR="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
mkdir -p "$TMP_DIR"
trap 'rm -rf "$TMP_DIR"' EXIT

python3 - "$TMP_DIR" <<'PY'
from pathlib import Path
import json
import os
import shlex
import subprocess
import sys

import pyarrow as pa
import pyarrow.ipc as ipc

out = Path(sys.argv[1])
local = shlex.split(os.environ["CLICKHOUSE_LOCAL"])
queries = []
checks = []


def make_struct(values, nulls, name="a", child_nullable=False):
    return pa.StructArray.from_arrays(
        [values], fields=[pa.field(name, values.type, nullable=child_nullable)], mask=pa.array(nulls))


# Parent nulls hide valid numeric text. Visible text must still be converted and validated.
strings = pa.array(["123", "456", "789"])
values = make_struct(strings, [True, False, False])
plain = [{"a": 0}, {"a": 456}, {"a": 789}]
nullable = [None, {"a": 456}, {"a": 789}]
cases = [
    ("plain", values, "Tuple(a Int32)", plain, 0, 0),
    ("nullable", values, "Nullable(Tuple(a Int32))", nullable, 1, 0),
    ("null_as_default", values, "Tuple(a Int32)", plain, 1, 1),
    ("all_null", make_struct(strings, [True, True, True]), "Tuple(a Int32)", [{"a": 0}] * 3, 0, 0),
    ("all_valid", make_struct(strings, [False, False, False]), "Tuple(a Int32)",
     [{"a": 123}, {"a": 456}, {"a": 789}], 0, 0),
    ("empty", values.slice(0, 0), "Nullable(Tuple(a Int32))", [], 1, 0),
    ("positional", values, "Tuple(Int32)", [[0], [456], [789]], 0, 0),
]

# A child's own null map remains independent of its parent's null map.
child_nulls = make_struct(pa.array(["123", None, "789"]), [True, False, False], child_nullable=True)
cases.append(("nullable_child", child_nulls, "Nullable(Tuple(a Nullable(Int32)))",
              [None, {"a": None}, {"a": 789}], 1, 0))
cases.append(("unchanged_child", child_nulls, "Tuple(a Nullable(String))",
              [{"a": None}, {"a": None}, {"a": "789"}], 0, 0))
inner = make_struct(strings, [False, True, False])
outer = make_struct(inner, [True, False, False], name="inner", child_nullable=True)
cases.append(("nested_plain", outer, "Tuple(inner Tuple(a Int32))",
              [{"inner": {"a": 0}}, {"inner": {"a": 0}}, {"inner": {"a": 789}}], 0, 0))
cases.append(("nested_nullable", outer, "Nullable(Tuple(inner Nullable(Tuple(a Int32))))",
              [None, {"inner": None}, {"inner": {"a": 789}}], 1, 0))
cases.append(("nested_positional", outer, "Tuple(Tuple(Int32))", [[[0]], [[0]], [[789]]], 0, 0))

# Container traversal preserves null structs inside array elements and map values.
arrays = pa.ListArray.from_arrays(pa.array([0, 2, 3]), values)
cases.append(("array", arrays, "Array(Tuple(a Int32))", [plain[:2], plain[2:]], 0, 0))
maps = pa.MapArray.from_arrays(pa.array([0, 2, 3]), pa.array(["x", "y", "z"]), values)
cases.append(("map", maps, "Map(String, Tuple(a Int32))",
              [{"x": plain[0], "y": plain[1]}, {"z": plain[2]}], 0, 0))

# Dictionary values and dictionary-encoded children follow the same conversion rules as inline structs.
dictionary = pa.DictionaryArray.from_arrays(pa.array([0, 1, 0]), values.slice(0, 2))
cases.append(("dictionary", dictionary, "Tuple(a Int32)", [plain[0], plain[1], plain[0]], 0, 0))
child_dictionary = pa.DictionaryArray.from_arrays(pa.array([0, 1, 2]), strings)
cases.append(("dictionary_child", make_struct(child_dictionary, [True, False, False]), "Tuple(a Int32)", plain, 0, 0))

# Named fields retain their requested order, and unrequested fields do not require conversion.
reordered = pa.StructArray.from_arrays(
    [strings, pa.array(["10", "20", "30"])],
    fields=[pa.field("a", pa.string(), nullable=False), pa.field("b", pa.string(), nullable=False)],
    mask=pa.array([True, False, False]))
cases.append(("reordered", reordered, "Tuple(b Int32, a Int32)",
              [{"b": 0, "a": 0}, {"b": 20, "a": 456}, {"b": 30, "a": 789}], 0, 0))
cases.append(("subset", reordered, "Tuple(b Int32)", [{"b": 0}, {"b": 20}, {"b": 30}], 0, 0))
cases.append(("missing", reordered, "Tuple(c Int32)", [{"c": 0}] * 3, 0, 0))
# Missing fields use their declared type's default, including enums whose first value is nonzero.
cases.append(("missing_enum", make_struct(strings, [False, False, False]),
              "Tuple(status Enum8('ready' = 1, 'done' = 2))", [{"status": "ready"}] * 3, 0, 0))

# Null structs use the destination tuple's defaults, including enums and nullable elements.
enum_target = "Tuple(a Enum8('ready' = 1, 'done' = 2))"
enum_values = make_struct(pa.array(["ready", "done"]), [True, False])
enum_defaults = [{"a": "ready"}, {"a": "done"}]
cases.append(("enum_default", enum_values, enum_target, enum_defaults, 0, 0))
cases.append(("enum_null_as_default", enum_values, enum_target, enum_defaults, 1, 1))
cases.append(("enum_nullable", enum_values, f"Nullable({enum_target})", [None, {"a": "done"}], 1, 0))
cases.append(("enum_all_null", make_struct(pa.array(["ready", "done"]), [True, True]),
              enum_target, [{"a": "ready"}] * 2, 0, 0))
cases.append(("missing_enum_null", values, "Tuple(status Enum8('ready' = 1, 'done' = 2))",
              [{"status": "ready"}] * 3, 0, 0))

for fmt, writer, reader in (("Arrow", ipc.new_file, ipc.open_file),
                            ("ArrowStream", ipc.new_stream, ipc.open_stream)):
    for name, column, target, expected, nullable_tuples, null_as_default in cases:
        path = out / f"{name}.{fmt}"
        batch = pa.record_batch([column], names=["s"])
        with writer(path, batch.schema) as stream:
            stream.write_batch(batch)
        assert reader(path).read_all().to_pydict() == batch.to_pydict()
        structure = ("s " + target).replace("'", "''")
        queries.append(
            f"SELECT s FROM file('{path}', '{fmt}', '{structure}') FORMAT JSONEachRow "
            f"SETTINGS allow_experimental_nullable_tuple_type={nullable_tuples}, "
            f"input_format_null_as_default={null_as_default};")
        checks.append((f"{fmt} {name}: OK", expected))

    # Column defaults apply to null input tuples when null-as-default handling is enabled.
    for nullable_tuples in (0, 1):
        queries.extend([
            f"SET allow_experimental_nullable_tuple_type={nullable_tuples}, input_format_null_as_default=1, "
            "input_format_defaults_for_omitted_fields=1;",
            "CREATE TEMPORARY TABLE default_rows (s Tuple(a Int32) DEFAULT tuple(99));",
            f"INSERT INTO default_rows FROM INFILE '{out / f'plain.{fmt}'}' FORMAT {fmt};",
            "SELECT s FROM default_rows FORMAT JSONEachRow;",
            "DROP TABLE default_rows;",
        ])
        checks.append((f"{fmt} column_default_{nullable_tuples}: OK", [{"a": 99}, {"a": 456}, {"a": 789}]))
    queries.append("SET input_format_null_as_default=0, input_format_defaults_for_omitted_fields=0;")

    queries.append(
        f"SELECT s FROM file('{out / f'plain.{fmt}'}', '{fmt}', 's Tuple(a Int32)') "
        "SETTINGS allow_experimental_nullable_tuple_type=1; -- { serverError CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN }")
    checks.append((f"{fmt} null_tuple: rejected", []))

    invalid = make_struct(pa.array(["123", "invalid"]), [True, False])
    path = out / f"visible_text.{fmt}"
    batch = pa.record_batch([invalid], names=["s"])
    with writer(path, batch.schema) as stream:
        stream.write_batch(batch)
    queries.append(
        f"SELECT s FROM file('{path}', '{fmt}', 's Nullable(Tuple(a Int32))') "
        "SETTINGS allow_experimental_nullable_tuple_type=1; -- { serverError CANNOT_PARSE_TEXT }")
    checks.append((f"{fmt} visible_text: rejected", []))

result = subprocess.run(local + [
    "--path", str(out / "local"), "--max_threads=1", "--output_format_json_named_tuples_as_objects=1",
    "--multiquery", "--query", "\n".join(queries),
], text=True, capture_output=True)
assert result.returncode == 0 and not result.stderr, (result.returncode, result.stderr)
lines = result.stdout.splitlines()
position = 0
for name, expected in checks:
    actual = [json.loads(line)["s"] for line in lines[position:position + len(expected)]]
    assert actual == expected, (name, actual, expected)
    position += len(expected)
    print(name)
assert position == len(lines), lines[position:]
PY
