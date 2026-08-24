#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_LOCAL -q "desc file('$CUR_DIR/data_avro/decimals.avro')"
$CLICKHOUSE_LOCAL -q "select * from file('$CUR_DIR/data_avro/decimals.avro')"

