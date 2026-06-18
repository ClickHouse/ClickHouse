#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: needs pyarrow to check record batch size.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

TMP_DIR=$(mktemp -d)
trap 'rm -rf "$TMP_DIR"' EXIT

check_batches()
{
    local format="$1"
    local filename="$TMP_DIR/out_${format}.arrow"

    "$CLICKHOUSE_CLIENT" --query "
        SELECT number
        FROM numbers(5)
        SETTINGS output_format_arrow_row_group_size = 2
        FORMAT ${format}
    " > "$filename"

    python3 - "$format" "$filename" <<'PY'
import sys
import pyarrow.ipc as ipc

fmt = sys.argv[1]
path = sys.argv[2]

with open(path, "rb") as f:
    if fmt == "ArrowStream":
        reader = ipc.open_stream(f)
    else:
        reader = ipc.open_file(f)

    print(fmt, [batch.num_rows for batch in reader])
PY
}

check_batches ArrowStream
check_batches Arrow

"$CLICKHOUSE_CLIENT" --query "
    SELECT number
    FROM numbers(1)
    SETTINGS output_format_arrow_row_group_size = 0
    FORMAT ArrowStream
" 2>&1 >/dev/null | grep -q "BAD_ARGUMENTS" && echo "zero rejected"
