#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: needs the pyarrow Python module to build the Arrow IPC stream.
#
# Regression test for the native Arrow IPC reader: a dictionary-encoded field whose value type is a dense
# union. The RecordBatch carries only the dictionary's index buffer (validity + indices); the union value
# layout lives in the DictionaryBatch. The reader must treat the field as dictionary-encoded before the
# union/null special cases, otherwise it reads the index buffer as a union type-id/offset buffer (and walks
# child nodes that are only present in the dictionary batch). Both the decode path and the skip/pruning path
# are covered: selecting another column must skip the dictionary<union> column without misreading its
# buffers.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

for FORMAT in ArrowStream Arrow; do
    DATA_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_${FORMAT}.arrow"

    python3 - "$DATA_FILE" "$FORMAT" <<'EOF'
import sys
import pyarrow as pa
import pyarrow.ipc as ipc

path, fmt = sys.argv[1], sys.argv[2]

# Dense union with an int32 and a string alternative; the dictionary indexes select the logical sequence
# [10, 'x', 10, 20]. Paired with a plain int32 column `k` so a projected read can skip the union column.
union = pa.UnionArray.from_dense(
    pa.array([0, 1, 0, 1], type=pa.int8()),
    pa.array([0, 0, 1, 1], type=pa.int32()),
    [pa.array([10, 20], type=pa.int32()), pa.array(['x', 'y'], type=pa.string())],
    ['i', 's'])
d = pa.DictionaryArray.from_arrays(pa.array([0, 1, 0, 2], type=pa.int32()), union)
k = pa.array([1, 2, 3, 4], type=pa.int32())

schema = pa.schema([pa.field('k', pa.int32()), pa.field('d', d.type)])
batch = pa.record_batch([k, d], schema=schema)

writer_factory = ipc.new_stream if fmt == 'ArrowStream' else ipc.new_file
w = writer_factory(path, schema)
w.write_batch(batch)
w.close()
EOF

    echo "--- ${FORMAT}: decode dictionary<dense_union> column ---"
    ${CLICKHOUSE_LOCAL} --input_format_arrow_use_native_reader=1 \
        --query "SELECT k, toTypeName(d), d FROM file('${DATA_FILE}', '${FORMAT}') ORDER BY k"

    echo "--- ${FORMAT}: projecting another column skips the dictionary<union> column ---"
    ${CLICKHOUSE_LOCAL} --input_format_arrow_use_native_reader=1 \
        --query "SELECT k FROM file('${DATA_FILE}', '${FORMAT}') ORDER BY k"

    rm -f "$DATA_FILE"
done
