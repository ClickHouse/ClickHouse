#!/usr/bin/env bash
# Tags: long, no-fasttest
# no-fasttest: needs the pyarrow Python module to build the Arrow streams.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

TMP_DIR="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
mkdir -p "$TMP_DIR"
trap 'rm -rf "$TMP_DIR"' EXIT

# A union row that selects a null value of a nullable child is a `Variant` NULL, whatever the child's type:
# a struct read as a plain `Tuple`, a list, or a primitive. `Array` and plain `Tuple` columns carry no null
# map of their own, so the child's validity must reach the union through the decoder rather than through a
# `Nullable` wrapper. Null values no row selects change nothing. Both union layouts are covered, and the
# struct child is read with and without nullable tuples allowed.

python3 - "$TMP_DIR" <<'PYEOF'
import sys
import pyarrow as pa
import pyarrow.ipc as ipc

out = sys.argv[1]
struct_type = pa.struct([pa.field("a", pa.int32())])
list_type = pa.list_(pa.int32())
names = ["st", "li", "n"]


def write(name, column):
    schema = pa.schema([pa.field("u", column.type)])
    with ipc.new_stream(f"{out}/{name}.arrows", schema) as writer:
        writer.write_batch(pa.record_batch([column], schema=schema))


type_ids = pa.array([0, 0, 0, 1, 1, 1, 2, 2, 2], type=pa.int8())
# Dense: each child holds three values, the middle one null, and every row selects one of them.
write("dense", pa.UnionArray.from_dense(
    type_ids, pa.array([0, 1, 2, 0, 1, 2, 0, 1, 2], type=pa.int32()),
    [pa.array([{"a": 1}, None, {"a": 3}], type=struct_type), pa.array([[1], None, [3]], type=list_type),
     pa.array([1, None, 3], type=pa.int64())], names))
# Dense: the null values sit in slots no row selects.
write("dense_unselected", pa.UnionArray.from_dense(
    type_ids, pa.array([0, 2, 3, 0, 2, 3, 0, 2, 3], type=pa.int32()),
    [pa.array([{"a": 1}, None, {"a": 2}, {"a": 3}], type=struct_type), pa.array([[1], None, [2], [3]], type=list_type),
     pa.array([1, None, 2, 3], type=pa.int64())], names))
# Sparse: every child spans all rows and is null in the middle row of its own selection.
write("sparse", pa.UnionArray.from_sparse(
    type_ids,
    [pa.array([{"a": 1}, None, {"a": 3}, {"a": 4}, {"a": 5}, {"a": 6}, {"a": 7}, {"a": 8}, {"a": 9}], type=struct_type),
     pa.array([[1], [2], [3], [4], None, [6], [7], [8], [9]], type=list_type),
     pa.array([1, 2, 3, 4, 5, 6, 7, None, 9], type=pa.int64())], names))
PYEOF

for NAME in dense dense_unselected sparse; do
    for NULLABLE_TUPLES in 0 1; do
        echo "--- ${NAME}, allow_experimental_nullable_tuple_type=${NULLABLE_TUPLES} ---"
        ${CLICKHOUSE_LOCAL} --allow_experimental_nullable_tuple_type=${NULLABLE_TUPLES} \
            --query "SELECT u, toTypeName(u) FROM file('${TMP_DIR}/${NAME}.arrows', 'ArrowStream')"
    done
done
