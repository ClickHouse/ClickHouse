#!/usr/bin/env bash
# Tags: no-fasttest
# Corrupt thrift-encoded Parquet metadata must surface as INCORRECT_DATA, not the generic
# STD_EXCEPTION (1001) it used to.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TMP_DIR="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
mkdir -p "$TMP_DIR"
trap 'rm -rf "$TMP_DIR"' EXIT

python3 - "$TMP_DIR" <<'PYEOF'
import struct, sys
import pyarrow as pa
import pyarrow.parquet as pq

out = sys.argv[1]
path = f"{out}/corrupt_page_header.parquet"
tbl = pa.table({"i": pa.array(list(range(500)), pa.int32())})
pq.write_table(tbl, path, compression="none")

d = bytearray(open(path, "rb").read())
# The first page's PageHeader starts right after the 4-byte 'PAR1' magic. Corrupting it leaves
# the footer (and hence schema inference) intact, so the failure is on the data-read path.
footer_len, = struct.unpack_from("<I", d, len(d) - 8)
data_end = len(d) - 8 - footer_len
for i in range(4, min(64, data_end)):
    d[i] = 0xFF
open(path, "wb").write(bytes(d))
PYEOF

out=$($CLICKHOUSE_LOCAL --query "
    SELECT * FROM file('${TMP_DIR}/corrupt_page_header.parquet', Parquet) FORMAT Null" 2>&1)

# All three arms must read the same capture: alone, the absence arm also holds for a query that
# failed for an unrelated reason.
echo "$out" | grep -F -q 'Code: 117' && echo 'code 117: yes' || echo 'code 117: no'
echo "$out" | grep -F -q 'INCORRECT_DATA' && echo 'INCORRECT_DATA: yes' || echo 'INCORRECT_DATA: no'
echo "$out" | grep -F -q 'STD_EXCEPTION' && echo 'STD_EXCEPTION: yes' || echo 'STD_EXCEPTION: no'
