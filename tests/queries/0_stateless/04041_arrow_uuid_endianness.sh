#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

DATA_FILE=$CLICKHOUSE_TEST_UNIQUE_NAME.arrow

$CLICKHOUSE_LOCAL -q "SELECT toUUID('4d8f1ec8-bc41-4148-8069-d31c399b507b') AS u FORMAT Arrow" > $DATA_FILE

python3 -c "
import pyarrow.feather as feather

table = feather.read_table('$DATA_FILE')
raw_bytes = table['u'][0].as_py()

print(raw_bytes)
"

rm -f $DATA_FILE
