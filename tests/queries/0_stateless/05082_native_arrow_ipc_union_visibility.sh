#!/usr/bin/env bash
# Tags: no-fasttest
# This test requires PyArrow to write dense and sparse unions.

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
local = shlex.split(os.environ["CLICKHOUSE_LOCAL"])
queries = []
checks = []
type_ids = pa.array([0, 1, 0, 1], type=pa.int8())

# Dense unions address compact children by offset, while sparse children retain the parent's row count.
# A null struct row hides its selected union value when the requested plain `Tuple` drops outer nullability.
for mode in ("dense", "sparse"):
    if mode == "dense":
        union = pa.UnionArray.from_dense(
            type_ids, pa.array([0, 0, 1, 1], type=pa.int32()),
            [pa.array([10, 30], type=pa.int32()), pa.array(["b", "d"])], ["i", "s"])
    else:
        union = pa.UnionArray.from_sparse(
            type_ids, [pa.array([10, 20, 30, 40], type=pa.int32()), pa.array(["a", "b", "c", "d"])],
            ["i", "s"])
    for nullable in (False, True):
        mask = pa.array([False, True, False, False]) if nullable else None
        column = pa.StructArray.from_arrays([union], fields=[pa.field("v", union.type)], mask=mask)
        batch = pa.record_batch([column], schema=pa.schema([pa.field("c", column.type, nullable=nullable)]))
        expected = [{"v": 10}, {"v": None if nullable else "b"}, {"v": 30}, {"v": "d"}]
        for fmt, writer_type in (("Arrow", ipc.new_file), ("ArrowStream", ipc.new_stream)):
            path = f"{out}/{mode}_{nullable}.{fmt}"
            with writer_type(path, batch.schema) as writer:
                writer.write_batch(batch)
            queries.append(
                f"SELECT toJSONString(c) FROM file('{path}', '{fmt}', 'c Tuple(v Variant(Int32, String))') FORMAT TSVRaw;")
            checks.append((f"OK {fmt} {mode} {'nullable' if nullable else 'non-nullable'}", expected))

result = subprocess.run(local + [
    "--path", f"{out}/local", "--max_threads=1", "--allow_experimental_nullable_tuple_type=0",
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
