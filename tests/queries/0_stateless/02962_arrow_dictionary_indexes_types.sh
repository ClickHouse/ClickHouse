#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

DATA_FILE=$CLICKHOUSE_TEST_UNIQUE_NAME.arrow

$CLICKHOUSE_LOCAL -q "select toLowCardinality(toString(number)) as lc from numbers(1000) format Arrow settings output_format_arrow_low_cardinality_as_dictionary=1, output_format_arrow_use_signed_indexes_for_dictionary=1, output_format_arrow_use_64_bit_indexes_for_dictionary=1" > $DATA_FILE
python3 -c "import pyarrow as pa; print(pa.ipc.open_file(pa.OSFile('$DATA_FILE', 'rb')).read_all().schema)"

$CLICKHOUSE_LOCAL -q "select toLowCardinality(toString(number)) as lc from numbers(1000) format Arrow settings output_format_arrow_low_cardinality_as_dictionary=1, output_format_arrow_use_signed_indexes_for_dictionary=1, output_format_arrow_use_64_bit_indexes_for_dictionary=0" > $DATA_FILE
python3 -c "import pyarrow as pa; print(pa.ipc.open_file(pa.OSFile('$DATA_FILE', 'rb')).read_all().schema)"

$CLICKHOUSE_LOCAL -q "select toLowCardinality(toString(number)) as lc from numbers(1000) format Arrow settings output_format_arrow_low_cardinality_as_dictionary=1, output_format_arrow_use_signed_indexes_for_dictionary=0, output_format_arrow_use_64_bit_indexes_for_dictionary=1" > $DATA_FILE
python3 -c "import pyarrow as pa; print(pa.ipc.open_file(pa.OSFile('$DATA_FILE', 'rb')).read_all().schema)"

$CLICKHOUSE_LOCAL -q "select toLowCardinality(toString(number)) as lc from numbers(1000) format Arrow settings output_format_arrow_low_cardinality_as_dictionary=1, output_format_arrow_use_signed_indexes_for_dictionary=0, output_format_arrow_use_64_bit_indexes_for_dictionary=0" > $DATA_FILE
python3 -c "import pyarrow as pa; print(pa.ipc.open_file(pa.OSFile('$DATA_FILE', 'rb')).read_all().schema)"

rm $DATA_FILE

