#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The Web UI (`programs/server/play.html`) used to have two layouts. A fresh page with one
# never-run tab hid the tab bar and showed the url/user/password inputs inline above the editor;
# everything else showed the tab bar and moved that same `#inputs` element into the drop-down
# behind the connection key. The page switched between them by relocating the element.
#
# There is one layout now. The bar is always shown - so its controls, in particular loading a
# saved workspace, are reachable on a first-ever visit - and the connection inputs always live in
# the drop-down, where the markup puts them from the start (no relocation, no first-paint shift).

URL="${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}"
page="$(${CLICKHOUSE_CURL} -sS "${URL}/play")"

echo '--- the tab bar and the connection key are never hidden'
echo "$page" | grep -oE '^ *<div id="tab-bar">$' | sed 's/^ *//'
echo "$page" | grep -oE '<button id="connection-key" type="button" title="[^"]*" aria-expanded="false">'
echo "$page" | grep -cE '^ *(tab_bar_elem|connection_key_elem)\.hidden = '

echo '--- the connection inputs are inside the drop-down in the markup, and never move'
echo "$page" | grep -A1 -E '^ *<div id="connection-dropdown" hidden>$' | sed 's/^ *//'
echo "$page" | grep -cE 'syncConnectionUI|controls_elem'

echo '--- so the surface a connection edit is committed on is the one menu that always exists'
echo "$page" | grep -A3 -F 'function connectionSurface() {' | grep -oF "return document.getElementById('connection-menu');"

echo '--- and a failed credential check always has a drop-down to open'
echo "$page" | grep -A1 -F "/// The credentials didn't work" | grep -oF 'openConnectionDropdown();'
