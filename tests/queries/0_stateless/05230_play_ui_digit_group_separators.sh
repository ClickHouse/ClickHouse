#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The Web UI (`programs/server/play.html`) groups the digits of a long number so that it reads as
# 1 234 567. The groups are separated by an EMPTY element rather than by a separator character: the
# gap is painted, but the text of the cell is still nothing but the digits, so selecting the number -
# and copying the cell - yields the digits alone. A thin space would be copied along with them.
#
# The markup is built by `digitGroupsHTML` and the gap comes from the `.num-sep` rule.

URL="${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}"
page="$(${CLICKHOUSE_CURL} -sS "${URL}/play")"

echo '--- the separator between two digit groups is an element with no content'
echo "$page" | grep -oF "return groups.join('<span class=\"num-sep\"></span>');" | head -n1

echo '--- its width is a quarter of a digit: an inline-block box measured in `ch`'
echo "$page" | grep -A3 -E '^ *\.num-sep$' | grep -oE '^ *(display: inline-block|width: 0\.25ch);$'

echo '--- the leading group is the short one, and the rest are groups of three'
echo "$page" | grep -oE '^ *const head = digits\.length % 3 \|\| 3;$' | head -n1
echo "$page" | grep -oE '^ *for \(let i = head; i < digits\.length; i \+= 3\) \{ groups\.push\(digits\.slice\(i, i \+ 3\)\); \}$' | head -n1

echo '--- a numeric cell of at least five digits is grouped (as in `clickhouse-client`), shorter ones are not'
echo "$page" | grep -oF 'if (this._column_is_number[name] && text.match(/^\d{5,}$/))' | head -n1
echo '--- and so is a row number of at least five digits'
echo "$page" | grep -oF 'if (row_number.length >= 5) { td.innerHTML = digitGroupsHTML(row_number); }' | head -n1

echo '--- the value the cell reports to the filter menu is the raw text, not the grouped rendering'
echo "$page" | grep -oE '^ *td\._filterText = text;$' | head -n1
