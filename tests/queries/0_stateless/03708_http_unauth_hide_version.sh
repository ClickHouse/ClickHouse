#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TMP_DIR=$(mktemp -d "$CUR_DIR"/03708_XXXXXX)
trap 'rm -rf "$TMP_DIR"' EXIT

URL="${CLICKHOUSE_PORT_HTTP_PROTO}://default:invalid_password@${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}/"

${CLICKHOUSE_CURL} -s -D "$TMP_DIR/headers" -o "$TMP_DIR/body" "$URL" --data-binary "SELECT 1" || true

head -n 1 "$TMP_DIR/headers" | sed 's/\r$//'
if grep -q "(version " "$TMP_DIR/body"; then
    echo "version_leaked"
else
    echo "no_version"
fi

# Now authenticate successfully and trigger an exception to ensure version is present.
GOOD_URL="${CLICKHOUSE_PORT_HTTP_PROTO}://default:@${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}/"
${CLICKHOUSE_CURL} -s -D "$TMP_DIR/headers2" -o "$TMP_DIR/body2" "$GOOD_URL" --data-binary "SELECT * FROM no_such_table" || true

head -n 1 "$TMP_DIR/headers2" | sed 's/\r$//'
if grep -q "(version " "$TMP_DIR/body2"; then
    echo "version_present_after_auth"
else
    echo "version_missing_after_auth"
fi
