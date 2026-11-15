#!/usr/bin/env bash
# Tags: no-fasttest
# Regression test: Iceberg/Paimon parse table-metadata schema JSON with Poco::JSON::Parser, which
# defaulted to unlimited depth (setDepth was a no-op), so a deeply nested schema overflowed the
# native stack inside Poco's recursive parser. The parsers now set a depth limit, so deep nesting is
# rejected with a clean JSON exception instead of crashing.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TMP_DIR="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
mkdir -p "$TMP_DIR"
trap 'rm -rf "$TMP_DIR"' EXIT

# Reads a query's combined output from stdin. If it contains $2, print a stable "<label>: <marker>"
# line; otherwise print the actual output so a CI failure shows what really happened instead of FAIL.
expect_contains() {
    local label="$1" marker="$2" out
    out=$(cat)
    if printf '%s\n' "$out" | grep -qF "$marker"; then
        echo "$label: $marker"
    else
        echo "$label: expected '$marker', got:"
        printf '%s\n' "$out" | head -3
    fi
}

# Iceberg parses the whole metadata JSON with Poco before any field validation, so a deeply nested
# metadata file overflows the parser (reachable at default settings, no kernel involved). Must be
# rejected with a JSON exception.
mkdir -p "$TMP_DIR/ice/metadata"
python3 -c "
N = 2000
open('$TMP_DIR/ice/metadata/v1.metadata.json', 'wb').write(b'{' + b'\"a\":{' * N + b'\"x\":1' + b'}' * N + b'}')
open('$TMP_DIR/ice/metadata/version-hint.text', 'w').write('1')
"
$CLICKHOUSE_LOCAL --query "DESCRIBE TABLE icebergLocal('$TMP_DIR/ice') FORMAT Null" 2>&1 \
    | expect_contains iceberg_depth "Maximum allowed JSON depth exceeded"

# Paimon parses its schema file (schema/schema-N) with Poco before validation; a deeply nested
# schema must be rejected with a JSON exception, not crash (reachable at default settings).
mkdir -p "$TMP_DIR/pm/schema"
python3 -c "
N = 2000
open('$TMP_DIR/pm/schema/schema-0', 'wb').write(b'{' + b'\"a\":{' * N + b'\"x\":1' + b'}' * N + b'}')
"
$CLICKHOUSE_LOCAL --query "DESCRIBE TABLE paimonLocal('$TMP_DIR/pm') FORMAT Null" 2>&1 \
    | expect_contains paimon_depth "Maximum allowed JSON depth exceeded"
