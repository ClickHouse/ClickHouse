#!/usr/bin/env bash
# Tags: no-fasttest

# Verify that cached schema filenames contain a hex-encoded content sample prefix
# that differs between schemas with different content.
# Regression test for https://github.com/ClickHouse/ClickHouse/issues/101904

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

WORKDIR="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "$WORKDIR"
mkdir -p "$WORKDIR"

# Two schemas that differ within the first 32 bytes so their content sample prefixes differ.
SCHEMA1='syntax = "proto3"; message A04094 { uint64 x = 1; }'
SCHEMA2='syntax = "proto3"; message B04094 { string y = 1; }'

# Each will create a cached file under __cache__ with a filename of the form:
#   <hex_content_sample>-<hash>.proto
# Before the fix, the hex content sample was always the same (encoding of zero bytes).

(cd "$WORKDIR" && ${CLICKHOUSE_LOCAL} --logger.console=0 --query "
    SELECT 42 AS x FORMAT Protobuf
    SETTINGS format_schema_source = 'string',
             format_schema = '$SCHEMA1',
             format_schema_message_name = 'A04094'
" > /dev/null)

(cd "$WORKDIR" && ${CLICKHOUSE_LOCAL} --logger.console=0 --query "
    SELECT 'hello' AS y FORMAT Protobuf
    SETTINGS format_schema_source = 'string',
             format_schema = '$SCHEMA2',
             format_schema_message_name = 'B04094'
" > /dev/null)

CACHE_DIR="$WORKDIR/__cache__"

# Extract the content sample prefix (before the first '-') from cached filenames.
PREFIXES=$(ls "$CACHE_DIR"/*.proto 2>/dev/null | xargs -I{} basename {} | sed 's/-.*//' | sort -u)
NUM_UNIQUE=$(echo "$PREFIXES" | wc -l)

if [ "$NUM_UNIQUE" -ge 2 ]; then
    echo "OK"
else
    echo "FAIL: expected different content sample prefixes for different schemas, got: $PREFIXES"
fi

rm -rf "$WORKDIR"
