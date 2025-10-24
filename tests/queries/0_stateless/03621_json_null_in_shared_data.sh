#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_LOCAL -q "select '{\"a\" : null}'::JSON(a Dynamic) format RowBinary" | $CLICKHOUSE_LOCAL -q "select * from table format RowBinary" --input-format RowBinary --structure "json JSON(max_dynamic_paths=0)" | hexdump -C
