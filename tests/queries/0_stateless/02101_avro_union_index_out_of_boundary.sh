#!/usr/bin/env bash
# Tags: no-fasttest

set -e

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_DIR=$CUR_DIR/data_avro
cat "$DATA_DIR"/nested_complex_incorrect_data.avro | ${CLICKHOUSE_LOCAL} --input-format Avro --output-format CSV -S "\"b.b2_null_str\" Nullable(String)"    -q 'select * from table;' 2>&1 | grep -i 'index out of boundary' -o
