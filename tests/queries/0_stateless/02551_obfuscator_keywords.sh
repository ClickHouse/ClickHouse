#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

obf="$CLICKHOUSE_FORMAT --obfuscate"

echo "select 1 order by 1 with fill step 1" | $obf
echo "SELECT id,  mannWhitneyUTest(id) FROM id" | $obf
echo "SELECT 1 IS NULL" | $obf
