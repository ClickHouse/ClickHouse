#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

INPUT_FILE=$CUR_DIR/$CLICKHOUSE_DATABASE.tsv
echo "foo" > "$INPUT_FILE"

$CLICKHOUSE_CLIENT --external --file="$INPUT_FILE" --name=t --structure='x String' -m -q "
select * from t;
select * from t;
"

rm "${INPUT_FILE:?}"
