#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Test that JSON data with '=' in values is not misdetected as TSKV format.
# https://github.com/ClickHouse/ClickHouse/issues/100797

DATA_FILE=$CLICKHOUSE_TEST_UNIQUE_NAME.jsonl

echo '{"login":"alexey-milovidov","id":18581488,"node_id":"MDQ6VXNlcjE4NTgxNDg4","avatar_url":"https://avatars.githubusercontent.com/u/18581488?v=4"}' > "$DATA_FILE"

# Verify the file is detected as a JSON format, not TSKV.
${CLICKHOUSE_LOCAL} --query "DESCRIBE file('$DATA_FILE') FORMAT TSVRaw"

rm "$DATA_FILE"
