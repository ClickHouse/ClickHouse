#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Every textarea in the Web UI (`programs/server/play.html`) holds text a writing assistant has no
# business touching: SQL in the query editor, and a saved workspace - SQL again, plus Markdown and
# JSON - in the Save and Load dialogs. Grammarly in particular injects its own overlay into a
# textarea it has adopted, which both litters the monospace text with underlines and moves the
# caret around under the editor's own handlers.
#
# So each of them opts out of all of it: the browser's spell checker and autocorrect
# (`spellcheck`, `autocorrect`, `autocomplete`) and Grammarly (`data-gramm`).

URL="${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}"
page="$(${CLICKHOUSE_CURL} -sS "${URL}/play")"

tags="$(echo "$page" | grep -oE '<textarea [^>]*>')"

echo '--- how many textareas the page has, and how many opt out of each text engine'
echo "$tags" | wc -l
echo "$tags" | grep -c 'spellcheck="false"'
echo "$tags" | grep -c 'autocorrect="false"'
echo "$tags" | grep -c 'autocomplete="false"'
echo "$tags" | grep -c 'data-gramm="false"'

echo '--- the ones that opt out of all of them (a new textarea missing one drops out of this list)'
echo "$tags" | grep 'spellcheck="false"' | grep 'autocorrect="false"' | grep 'autocomplete="false"' \
    | grep 'data-gramm="false"' | grep -oE 'id="[^"]+"'
