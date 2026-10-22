#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `URL PREFIX` matches as a base path on a path-segment boundary, mirroring the `url_prefix` rule of
# configuration-defined handlers: '/api/v1' matches '/api/v1', '/api/v1/' and '/api/v1/write', but not
# '/api/v1beta'. The ambiguity check follows the same rule.

BASE="${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}"

# Per-test-unique names and URL prefix so parallel tests do not interfere (handlers are global).
DB="${CLICKHOUSE_DATABASE}"
P="/h_${DB}"
HPFX="hpfx_${DB}"
HBETA="hbeta_${DB}"

cleanup() {
    $CLICKHOUSE_CLIENT -q "DROP HANDLER IF EXISTS \`$HPFX\`; DROP HANDLER IF EXISTS \`$HBETA\`; DROP HANDLER IF EXISTS \`hdup_${DB}\`; DROP HANDLER IF EXISTS \`hsib_${DB}\`"
}
trap cleanup EXIT
cleanup

$CLICKHOUSE_CLIENT -q "CREATE HANDLER \`$HPFX\` URL PREFIX '${P}/api/v1' AS SELECT 'v1' AS r FORMAT TSV"

echo "=== the base path itself matches ==="
${CLICKHOUSE_CURL} -sS "${BASE}${P}/api/v1"

echo "=== the base path with a trailing slash matches ==="
${CLICKHOUSE_CURL} -sS "${BASE}${P}/api/v1/"

echo "=== a path below the base matches ==="
${CLICKHOUSE_CURL} -sS "${BASE}${P}/api/v1/write"

echo "=== a longer segment with the same byte prefix does not match ==="
${CLICKHOUSE_CURL} -sS -o /dev/null -w '%{http_code}\n' "${BASE}${P}/api/v1beta"

echo "=== an exact handler on the longer segment is not ambiguous with the prefix ==="
$CLICKHOUSE_CLIENT -q "CREATE HANDLER \`$HBETA\` URL '${P}/api/v1beta' AS SELECT 'beta' AS r FORMAT TSV"
${CLICKHOUSE_CURL} -sS "${BASE}${P}/api/v1beta"

echo "=== an exact URL under the base is ambiguous with the prefix ==="
$CLICKHOUSE_CLIENT -q "CREATE HANDLER \`hdup_${DB}\` URL '${P}/api/v1/sub' AS SELECT 1" 2>&1 | grep -o "AMBIGUOUS_HANDLER" | head -1

echo "=== an exact URL equal to the base is ambiguous with the prefix ==="
$CLICKHOUSE_CLIENT -q "CREATE HANDLER \`hdup_${DB}\` URL '${P}/api/v1' AS SELECT 1" 2>&1 | grep -o "AMBIGUOUS_HANDLER" | head -1

echo "=== a shorter prefix covering the base is ambiguous ==="
$CLICKHOUSE_CLIENT -q "CREATE HANDLER \`hdup_${DB}\` URL PREFIX '${P}/api' AS SELECT 1" 2>&1 | grep -o "AMBIGUOUS_HANDLER" | head -1

echo "=== a prefix on a disjoint sibling segment is not ambiguous ==="
$CLICKHOUSE_CLIENT -q "CREATE HANDLER \`hsib_${DB}\` URL PREFIX '${P}/api/v1b' AS SELECT 'v1b' AS r FORMAT TSV"
${CLICKHOUSE_CURL} -sS "${BASE}${P}/api/v1b/x"
$CLICKHOUSE_CLIENT -q "DROP HANDLER \`hsib_${DB}\`"

echo "=== a trailing slash in the prefix is ignored ==="
$CLICKHOUSE_CLIENT -q "ALTER HANDLER \`$HPFX\` URL PREFIX '${P}/api/v2/'"
${CLICKHOUSE_CURL} -sS "${BASE}${P}/api/v2"
${CLICKHOUSE_CURL} -sS -o /dev/null -w '%{http_code}\n' "${BASE}${P}/api/v2beta"
