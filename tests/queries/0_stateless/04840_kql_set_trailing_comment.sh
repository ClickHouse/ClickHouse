#!/usr/bin/env bash
# The `SET` fast path of `parseKQLQuery` skipped only `;` and whitespace after the statement, so a
# trailing comment looked like a second statement: with the gate on `SET dialect = 'clickhouse';`
# plus a comment threw `Multi-statements are not allowed`, and with the gate off the escape hatch
# of `tryParseKQLSetStatement` did not fire, leaving the session stuck behind `SUPPORT_IS_DISABLED`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

url_gate_on="${CLICKHOUSE_URL}&dialect=kusto&allow_experimental_kusto_dialect=1"
url_gate_off="${CLICKHOUSE_URL}&dialect=kusto&allow_experimental_kusto_dialect=0"

echo '-- a comment after the SET is not a second statement, with the gate on --'
${CLICKHOUSE_CURL} -sS "${url_gate_on}" --data-binary $'set max_threads = 1; // switch back' && echo OK
${CLICKHOUSE_CURL} -sS "${url_gate_on}" --data-binary $'set dialect = \'clickhouse\';\n// switch back' && echo OK
${CLICKHOUSE_CURL} -sS "${url_gate_on}" --data-binary $'set dialect = \'clickhouse\';\n-- switch back' && echo OK

echo '-- and not with the gate off either, so the escape hatch still fires --'
${CLICKHOUSE_CURL} -sS "${url_gate_off}" --data-binary $'set dialect = \'clickhouse\';\n// switch back' && echo OK
${CLICKHOUSE_CURL} -sS "${url_gate_off}" --data-binary $'set max_threads = 1; // switch back' && echo OK

echo '-- a real second statement behind a comment is still rejected --'
${CLICKHOUSE_CURL} -sS "${url_gate_on}" --data-binary $'set max_threads = 1;\n// switch back\nprint 1' | grep -c 'Multi-statements are not allowed'
${CLICKHOUSE_CURL} -sS "${url_gate_off}" --data-binary $'set dialect = \'clickhouse\';\n// switch back\nselect 1' | grep -c 'SUPPORT_IS_DISABLED'

echo '-- a comment after an ordinary KQL statement is not a second statement either --'
${CLICKHOUSE_CURL} -sS "${url_gate_on}" --data-binary $'print 1;\n// done'
