#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# In the Web UI, a selected cell of a `JSON` column shows its value pretty-printed (two-space
# indentation) and highlighted, instead of the compact single line the row is rendered with.
# The rendering and its styling are written in different places of the page and nothing fails
# when they drift apart: a token class the script emits but the stylesheet does not style is
# silently unhighlighted, and a palette variable defined for one theme only is silently missing
# in the other. This test pins those contracts on the served page.

PLAY_PAGE=$(${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}/play")

UNINDENTED_PAGE=$(echo "$PLAY_PAGE" | sed -e 's/^[[:space:]]*//')

# The pretty-printer exists, and a `JSON` column is what it is applied to.
echo "$UNINDENTED_PAGE" | grep -c -x -F 'function prettyPrintJSON(value)'
echo "$UNINDENTED_PAGE" | grep -c -F "this._column_is_json[elem.name] = !!unwrapped_type.match(/^JSON\\b/);"

# Two spaces per nesting level: the only place the indentation step is defined.
echo "$UNINDENTED_PAGE" | grep -c -x -F "const inner = indent + '  ';"

# Every token class the pretty-printer emits is styled inside the pretty-printed block.
for cls in $(echo "$UNINDENTED_PAGE" | sed -n -e "s/^.*token('\(json-[a-z]*\)',.*$/\1/p" | sort -u)
do
    if echo "$UNINDENTED_PAGE" | grep -q -F ".json-pretty .${cls} {"
    then
        echo "${cls} is emitted and styled"
    else
        echo "${cls} is emitted but not styled"
    fi
done

# The selected cell's overlay is color-inverted, so the pretty-printed block inverts itself back and
# has its own palette; every variable of that palette is defined for both themes (twice on the page).
echo "$UNINDENTED_PAGE" | grep -A 2 -x -F -m1 '.json-pretty' | grep -o -F -m1 'filter: invert(1);'
for var in $(echo "$UNINDENTED_PAGE" | grep -o -E 'var\(--json-[a-z]+\)' | grep -o -E -- '--json-[a-z]+' | sort -u)
do
    echo "${var}: $(echo "$UNINDENTED_PAGE" | grep -c -E "^${var}: ")"
done

# Expanding on selection has its counterpart on both ways the selection leaves a cell (moving to
# another cell and clearing it), so a cell never stays pretty-printed once deselected.
echo "$UNINDENTED_PAGE" | grep -c -x -F 'this._expandJSON(td);'
echo "$UNINDENTED_PAGE" | grep -c -x -F 'this._collapseJSON(this._current_selected_cell);'
