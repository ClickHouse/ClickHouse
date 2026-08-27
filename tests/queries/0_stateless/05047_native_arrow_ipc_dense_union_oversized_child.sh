#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: Arrow format and pyarrow are not available in fasttest builds

# A dense Arrow union's offsets must be monotonic per child, but a child may legitimately hold MORE
# values than the union references (e.g. a shared or sliced child). The decoder used to pass the
# child columns straight into ColumnVariant, whose size-per-discriminator invariant then failed with
# a LOGICAL_ERROR on such files (and on nullable children whose referenced nulls are translated to
# the Variant NULL discriminator). The referenced values must be compacted instead. The file is
# built with pyarrow because the ClickHouse writer always emits exactly-referenced children.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
trap 'rm -f "${DATA_FILE}.Arrow" "${DATA_FILE}.ArrowStream"' EXIT

python3 - "$DATA_FILE" <<'PY'
import sys

import pyarrow as pa

base = sys.argv[1]
# child 0 holds 7 int16 values but only offsets 0, 2, 4 and 6 are referenced; the 999s must not
# surface. child 1 is referenced completely.
union = pa.UnionArray.from_dense(
    pa.array([0, 0, 1, 0, 0], type=pa.int8()),
    pa.array([0, 2, 0, 4, 6], type=pa.int32()),
    [pa.array([10, 999, 30, 999, 50, 999, 70], type=pa.int16()),
     pa.array(['a'], type=pa.string())])
table = pa.table({'k': pa.array(range(5), type=pa.int64()), 'u': union})
for fmt, opener in [("Arrow", pa.ipc.new_file), ("ArrowStream", pa.ipc.new_stream)]:
    with pa.OSFile(f"{base}.{fmt}", "wb") as sink:
        with opener(sink, table.schema) as writer:
            writer.write_table(table)
PY

for FMT in Arrow ArrowStream
do
    echo "--- ${FMT}: only the referenced child values surface ---"
    ${CLICKHOUSE_LOCAL} -q "SELECT k, u FROM file('${DATA_FILE}.${FMT}', '${FMT}') ORDER BY k"
done
