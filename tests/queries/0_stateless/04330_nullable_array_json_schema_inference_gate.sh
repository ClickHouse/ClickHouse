#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e -o pipefail

echo '{"a": null}' > "$CLICKHOUSE_TEST_UNIQUE_NAME.json"
echo '{"a": [1, 2]}' >> "$CLICKHOUSE_TEST_UNIQUE_NAME.json"

$CLICKHOUSE_LOCAL -q "
DESC file('$CLICKHOUSE_TEST_UNIQUE_NAME.json', JSONEachRow)
SETTINGS allow_experimental_nullable_tuple_type = 1, allow_experimental_nullable_array_type = 0" | awk '{ print $1 "\t" $2 }'

rm "$CLICKHOUSE_TEST_UNIQUE_NAME.json"
