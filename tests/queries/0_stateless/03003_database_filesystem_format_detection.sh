#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


DATA_FILE=$CLICKHOUSE_TEST_UNIQUE_NAME.data

echo '{"a" : 42}' > $DATA_FILE
$CLICKHOUSE_LOCAL -q "desc table \`$DATA_FILE\`"
$CLICKHOUSE_LOCAL -q "select * from \`$DATA_FILE\`"

rm $DATA_FILE

