#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# An HTTP handler whose query is CREATE ROW POLICY binds the query parameters of the policy filter from the request.
# The same for CREATE MASKING POLICY; it is Cloud-only, so a value that does not parse shows that the parameter was bound.

BASE="${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}"
DB="${CLICKHOUSE_DATABASE}"
H="hrp_${DB}"
P="/hrp_${DB}"
HM="hmp_${DB}"
PM="/hmp_${DB}"

cleanup() {
    $CLICKHOUSE_CLIENT -q "DROP HANDLER IF EXISTS \`$H\`; DROP HANDLER IF EXISTS \`$HM\`; DROP ROW POLICY IF EXISTS p ON ${DB}.t"
}
trap cleanup EXIT
cleanup

$CLICKHOUSE_CLIENT -q "
CREATE TABLE ${DB}.t (k UInt64) ENGINE = Memory;
INSERT INTO ${DB}.t VALUES (1), (2);
CREATE HANDLER \`$H\` URL '${P}' METHODS (POST) AS CREATE ROW POLICY p ON ${DB}.t USING k > {v:UInt64} TO ALL;
CREATE HANDLER \`$HM\` URL '${PM}' METHODS (POST) AS CREATE MASKING POLICY mp ON ${DB}.t UPDATE k = {v:UInt64} TO ALL"

${CLICKHOUSE_CURL} -sS -X POST "${BASE}${P}?param_v=1"
$CLICKHOUSE_CLIENT -q "SELECT k FROM ${DB}.t ORDER BY k"
${CLICKHOUSE_CURL} -sS -X POST "${BASE}${PM}?param_v=abc" | grep -o -E 'UNKNOWN_QUERY_PARAMETER|BAD_QUERY_PARAMETER|SUPPORT_IS_DISABLED'
