#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: needs the pyarrow Python module to build the Arrow files.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TMP_DIR="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
mkdir -p "$TMP_DIR"
trap 'rm -rf "$TMP_DIR"' EXIT

# An Arrow dictionary may hold null entries: the nullability of a dictionary-encoded field describes the
# encoded array — its index validity — while the dictionary values are an array of their own. A nullable
# field reads a null entry as NULL, whether a row is null through the index or through the entry it points
# at. A non-nullable field may carry unreferenced null entries in its dictionary, but a row pointing at one
# would be a null row, which the field rules out. The dictionary `x, NULL, y` is read below through both
# kinds of field, with and without a row pointing at the null entry, in both formats.

python3 - "$TMP_DIR" <<'PYEOF'
import sys
import pyarrow as pa
import pyarrow.ipc as ipc

out = sys.argv[1]
values = pa.array(["x", None, "y"], type=pa.utf8())


def write(name, indices, nullable):
    column = pa.DictionaryArray.from_arrays(pa.array(indices, type=pa.int32()), values)
    schema = pa.schema([pa.field("c", column.type, nullable=nullable)])
    for fmt, new_writer in (("Arrow", ipc.new_file), ("ArrowStream", ipc.new_stream)):
        with new_writer(f"{out}/{name}.{fmt}", schema) as writer:
            writer.write_batch(pa.record_batch([column], schema=schema))


write("nullable_referenced", [0, None, 1, 2], nullable=True)
write("nullable_unreferenced", [0, None, 2], nullable=True)
write("non_nullable_unreferenced", [0, 2, 0], nullable=False)
write("non_nullable_referenced", [0, 1, 2], nullable=False)
PYEOF

for FORMAT in Arrow ArrowStream; do
    echo "=== ${FORMAT}"
    echo "--- nullable field: one row is null through the index, another points at the null entry ---"
    ${CLICKHOUSE_LOCAL} --query "SELECT c, toTypeName(c) FROM file('${TMP_DIR}/nullable_referenced.${FORMAT}', '${FORMAT}')"
    echo "--- nullable field: no row points at the null entry ---"
    ${CLICKHOUSE_LOCAL} --query "SELECT c, toTypeName(c) FROM file('${TMP_DIR}/nullable_unreferenced.${FORMAT}', '${FORMAT}')"
    echo "--- non-nullable field: no row points at the null entry ---"
    ${CLICKHOUSE_LOCAL} --query "SELECT c, toTypeName(c) FROM file('${TMP_DIR}/non_nullable_unreferenced.${FORMAT}', '${FORMAT}')"
    echo "--- non-nullable field read as String ---"
    ${CLICKHOUSE_LOCAL} --query "SELECT c FROM file('${TMP_DIR}/non_nullable_unreferenced.${FORMAT}', '${FORMAT}', 'c String')"
    echo "--- non-nullable field: a row points at the null entry, rejected ---"
    ${CLICKHOUSE_LOCAL} --query "SELECT c FROM file('${TMP_DIR}/non_nullable_referenced.${FORMAT}', '${FORMAT}')" 2>&1 | grep -o "INCORRECT_DATA" | head -n 1
done
