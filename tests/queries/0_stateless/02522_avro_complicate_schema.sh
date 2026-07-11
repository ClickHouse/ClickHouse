#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest

set -e

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_DIR=$CUR_DIR/data_avro

$CLICKHOUSE_LOCAL -q "desc file('$DATA_DIR/complicated_schema.avro')"
$CLICKHOUSE_LOCAL -q "select * from file('$DATA_DIR/complicated_schema.avro')"
