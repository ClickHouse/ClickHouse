#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The Web UI (`programs/server/play.html`) sends the selected page as `limit` + `page`, so the server
# returns only that page's rows and the page's first row is the result's `limit * (page - 1) + 1`-th.
# The leading row-number column must therefore count from that offset: numbering every page from 1
# would label different rows with the same numbers and say the 30th row of a result is its 10th.
#
# The offset lives in `_rowNumberOffset` and is added to the per-page counter in `appendRow`.

URL="${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}"
page="$(${CLICKHOUSE_CURL} -sS "${URL}/play")"

echo '--- the row number of a rendered row is the offset of its page plus its index within the page'
echo "$page" | grep -oF "createTextNode(this._rowNumberOffset() + this._row_idx)" | head -n1

echo '--- the offset is the number of rows the server skipped for the page: size * (page - 1)'
echo "$page" | grep -oE '^ *return size \* \(page - 1\);$' | head -n1

echo '--- no page selected is the unpaginated first page, which starts at 1 (a zero offset)'
echo "$page" | grep -oE '^ *if \(!\(page >= 1\) \|\| !\(size >= 1\)\) \{ return 0; \}$' | head -n1
