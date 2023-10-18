#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_LOCAL -q "desc file('$CUR_DIR/data_json/twitter.jsonl')" 2>&1 | grep -c "ONLY_NULLS_WHILE_READING_SCHEMA"

