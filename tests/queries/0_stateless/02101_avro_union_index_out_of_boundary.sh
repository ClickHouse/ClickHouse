#!/usr/bin/env bash
# Tags: no-fasttest

set -e

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_DIR=$CUR_DIR/data_avro
cat "$DATA_DIR"/nested_complex_incorrect_data.avro | ${CLICKHOUSE_LOCAL} --input-format Avro --output-format CSV -S "\"b.b2_null_str\" Nullable(String)"    -q 'select * from table;' 2>&1 | grep -i 'index out of boundary' -o

# An out-of-range union index read into Variant, Nullable, with input_format_null_as_default, and from a one-branch union.
${CLICKHOUSE_LOCAL} -q "SELECT * FROM file('$DATA_DIR/union_index_out_of_range_variant.avro')" 2>&1 | grep -o 'index out of boundary' | head -n 1
${CLICKHOUSE_LOCAL} -q "SELECT * FROM file('$DATA_DIR/union_index_out_of_range_nullable.avro')" 2>&1 | grep -o 'index out of boundary' | head -n 1
${CLICKHOUSE_LOCAL} -q "SELECT * FROM file('$DATA_DIR/union_index_out_of_range_nullable.avro', 'Avro', 'u String, n Int32') SETTINGS input_format_null_as_default = 1" 2>&1 | grep -o 'index out of boundary' | head -n 1
${CLICKHOUSE_LOCAL} -q "SELECT * FROM file('$DATA_DIR/union_index_out_of_range_single.avro')" 2>&1 | grep -o 'index out of boundary' | head -n 1
