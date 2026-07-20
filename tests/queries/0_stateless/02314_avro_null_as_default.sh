#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest

set -e

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_DIR=$CUR_DIR/data_avro

$CLICKHOUSE_LOCAL -q "desc file('$DATA_DIR/nullable_array.avro') settings input_format_avro_null_as_default=1"
$CLICKHOUSE_LOCAL -q "select * from file('$DATA_DIR/nullable_array.avro') settings input_format_avro_null_as_default=1"

$CLICKHOUSE_CLIENT -q "insert into function file(data_02314.avro) select NULL::Nullable(UInt32) as x from numbers(3) settings engine_file_truncate_on_insert=1"
$CLICKHOUSE_CLIENT -q "select * from file(data_02314.avro, auto, 'x UInt32') settings input_format_avro_null_as_default=1"

