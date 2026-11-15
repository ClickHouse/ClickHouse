#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The Web UI paints the text selection of the query editor itself once the editor loses focus:
# browsers stop painting it there, which left the range that `Run selected` acts on invisible.
# The selection is mirrored onto the `#query-backdrop` (the layer the user actually sees) with the
# CSS Custom Highlight API, so the highlight name in the stylesheet and in the script must agree -
# renaming one of them silently disables the painting, with nothing failing anywhere.

PLAY_PAGE=$(${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}/play")

UNINDENTED_PAGE=$(echo "$PLAY_PAGE" | sed -e 's/^[[:space:]]*//')

CSS_NAME=$(echo "$UNINDENTED_PAGE" | sed -n -e 's/^#query-backdrop::highlight(\(.*\))$/\1/p' | head -n 1)
JS_NAME=$(echo "$UNINDENTED_PAGE" | sed -n -e "s/^const INACTIVE_SELECTION_HIGHLIGHT = '\(.*\)';$/\1/p" | head -n 1)

if [ -n "$CSS_NAME" ] && [ "$CSS_NAME" = "$JS_NAME" ]
then
    echo "the highlight '$CSS_NAME' is both styled and registered"
else
    echo "the highlight is styled as '$CSS_NAME' but registered as '$JS_NAME'"
fi

# It must look exactly like the native selection, whichever theme is in use, so it takes its colors
# from the same two variables - checked inside the highlight's own block, because the very same
# declarations also appear in the `::selection` rule above it.
HIGHLIGHT_BLOCK=$(echo "$UNINDENTED_PAGE" | grep -A 4 -F -m1 '#query-backdrop::highlight(' | tr '\n' ' ' | tr -s ' ')

echo "$HIGHLIGHT_BLOCK" | grep -o -F -m1 'color: var(--selection-color);'
echo "$HIGHLIGHT_BLOCK" | grep -o -F -m1 'background-color: var(--selection-background-color);'

# Every backdrop re-render replaces its content and so invalidates the ranges of the highlight:
# both the painting path and the path that hides the backdrop have to refresh it.
echo "$UNINDENTED_PAGE" | grep -c -x -F 'highlightInactiveSelection();'
