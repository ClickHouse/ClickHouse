#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A real-time preview (the `query_result_previews` setting) fully replaces the previous one, so the
# Web UI must also take a preview OFF the screen in the two cases where no further preview and no
# result row follows it: an EMPTY `preview` packet - the intermediate result was emptied by
# `HAVING`, `OFFSET`, or `LIMIT` - and a query that fails before the first real row. Both are
# silent failures on the screen (stale rows shown as if they were the result), so they are checked
# on the served page itself.

PLAY_PAGE=$(${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}/play")

UNINDENTED_PAGE=$(echo "$PLAY_PAGE" | sed -e 's/^[[:space:]]*//')

echo '--- the preview can be cleared without waiting for the real result'
echo "$UNINDENTED_PAGE" | grep -c -x -F 'clearPreview()'

echo '--- an empty preview packet clears the previous preview'
echo "$UNINDENTED_PAGE" | grep -c -x -F 'else targetResultEl.clearPreview();'

echo '--- an exception clears the preview left by the failed query'
echo "$UNINDENTED_PAGE" | grep -c -x -F 'targetResultEl.clearPreview();'

echo '--- the real result is never cleared as a preview'
echo "$UNINDENTED_PAGE" | grep -A 3 -F -m1 'clearPreview()' | grep -c -F 'if (this._received_real_meta) return;'
