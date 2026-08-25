#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: Requires libraries

set -e

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_DIR=$CUR_DIR/data_avro

# See https://github.com/ClickHouse/ClickHouse/issues/60438
$CLICKHOUSE_LOCAL -q "DESC file('$DATA_DIR/negative_block_size_arrays.avro')"
$CLICKHOUSE_LOCAL -q "SELECT arraySum(arrayMap(x -> length(x), str_array)) AS res FROM file('$DATA_DIR/negative_block_size_arrays.avro')"
