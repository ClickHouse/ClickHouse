#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The Web UI (`programs/server/play.html`) can save the whole workspace - the list of tabs, the
# query of every tab and its query parameters - as a Markdown document, and load it back. The two
# buttons sit at the right end of the tab bar, left of the connection key, and each opens a modal:
# Save shows the document with Copy / Download, Load takes a pasted or uploaded document.
#
# The document has one top-level heading per tab, the tab's query in a fenced code block, its
# parameter values as JSON under a `## Parameters` sub-heading, and - when the tab has one - its
# result as a Markdown table. Only the headings, the queries and the parameters are read back: a
# result table is a lossy rendering of a result (cells escaped for the table syntax, no column
# types, rows capped), so loading one as data would fabricate a result the server never returned.

URL="${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}"
page="$(${CLICKHOUSE_CURL} -sS "${URL}/play")"

echo '--- the two buttons sit in the tab bar, in that order, before the connection key'
echo "$page" | grep -oE '<button id="tab-(save|load)" type="button" title="[^"]*">'
echo "$page" | grep -E '^ *<div id="(tab-io|connection-menu)">$' | sed 's/^ *//'

echo '--- one auto margin pushes that whole right-hand cluster over; a second one would split it'
echo "$page" | grep -cE '^ *margin-left: auto;$'

echo '--- Save opens a modal with the document, Load one with a textarea to paste into'
echo "$page" | grep -oE '^ *<dialog id="tabs-(save|load)-dialog" class="tabs-dialog">$' | sed 's/^ *//'
echo "$page" | grep -oE 'id="tabs-(save|load)-text"'
echo "$page" | grep -oE '^ *<button id="tabs-(save-copy|save-download|load-upload|load-apply)" type="button">[^<]*</button>$' | sed 's/^ *//'

echo '--- a section per tab: its title as a top-level heading, then its query in a fenced block'
echo "$page" | grep -oF "out.push('# ' + tab.title);"
echo "$page" | grep -oF "out.push(fence + '\n' + tab.query + '\n' + fence);"

echo '--- the fence is one backtick longer than the longest run of backticks inside the query'
echo "$page" | grep -oF "return '\`'.repeat(Math.max(3, longest + 1));"

echo '--- the parameter values follow the query, written as JSON so that they round-trip exactly'
echo "$page" | grep -oF "out.push('## Parameters');"
echo "$page" | grep -oF "out.push(params_fence + 'json\n' + json + '\n' + params_fence);"

echo '--- a cell of a result table escapes what would otherwise break the row apart'
echo "$page" | grep -oF "return text.replace(/\\\\/g, '\\\\\\\\').replace(/([\`|])/g, '\\\\\$1').replace(/\r?\n/g, '<br>');"

echo '--- loading reads back the title, the query and the parameters - and nothing else'
echo "$page" | grep -oF "if (kind === 'query') {"
echo "$page" | grep -oF "if (current && line.match(/^##[ \t]+parameters[ \t]*\$/i)) { expect = 'params'; continue; }" | sed 's/^ *//'
echo "$page" | grep -oF ".map(entry => ({ title: entry.title, query: entry.query ?? '', params: entry.params }));"

echo '--- a loaded tab states its query and its parameter values together, so the pair is coherent'
echo "$page" | grep -oF 'tab.params = entry.params;'
echo "$page" | grep -A4 -F 'tab.params = entry.params;' | grep -oF 'tab.paramsSyncedQuery = tab.query;'

echo '--- replacing the workspace closes the old tabs, and asks first when there is anything to lose'
echo "$page" | grep -oF 'if (has_content && !confirm('
echo "$page" | grep -A11 -F 'if (has_content && !confirm(' | grep -oF 'abortTabQuery(tab);'
