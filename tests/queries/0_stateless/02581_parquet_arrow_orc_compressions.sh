#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -o pipefail

$CLICKHOUSE_LOCAL -q "select * from numbers(10) format Parquet settings output_format_parquet_compression_method='none'" | $CLICKHOUSE_LOCAL --input-format=Parquet -q "select count() from table"
$CLICKHOUSE_LOCAL -q "select * from numbers(10) format Parquet settings output_format_parquet_compression_method='lz4'" | $CLICKHOUSE_LOCAL --input-format=Parquet -q "select count() from table"
$CLICKHOUSE_LOCAL -q "select * from numbers(10) format Parquet settings output_format_parquet_compression_method='snappy'" | $CLICKHOUSE_LOCAL --input-format=Parquet -q "select count() from table"
$CLICKHOUSE_LOCAL -q "select * from numbers(10) format Parquet settings output_format_parquet_compression_method='zstd'" | $CLICKHOUSE_LOCAL --input-format=Parquet -q "select count() from table"
$CLICKHOUSE_LOCAL -q "select * from numbers(10) format Parquet settings output_format_parquet_compression_method='brotli'" | $CLICKHOUSE_LOCAL --input-format=Parquet -q "select count() from table"
$CLICKHOUSE_LOCAL -q "select * from numbers(10) format Parquet settings output_format_parquet_compression_method='gzip'" | $CLICKHOUSE_LOCAL --input-format=Parquet -q "select count() from table"

$CLICKHOUSE_LOCAL -q "select * from numbers(10) format ORC settings output_format_orc_compression_method='none'" | $CLICKHOUSE_LOCAL --input-format=ORC -q "select count() from table"
$CLICKHOUSE_LOCAL -q "select * from numbers(10) format ORC settings output_format_orc_compression_method='lz4'" | $CLICKHOUSE_LOCAL --input-format=ORC -q "select count() from table"
$CLICKHOUSE_LOCAL -q "select * from numbers(10) format ORC settings output_format_orc_compression_method='zstd'" | $CLICKHOUSE_LOCAL --input-format=ORC -q "select count() from table"
$CLICKHOUSE_LOCAL -q "select * from numbers(10) format ORC settings output_format_orc_compression_method='zlib'" | $CLICKHOUSE_LOCAL --input-format=ORC -q "select count() from table"
$CLICKHOUSE_LOCAL -q "select * from numbers(10) format ORC settings output_format_orc_compression_method='snappy'" | $CLICKHOUSE_LOCAL --input-format=ORC -q "select count() from table"


$CLICKHOUSE_LOCAL -q "select * from numbers(10) format Arrow settings output_format_arrow_compression_method='none'" | $CLICKHOUSE_LOCAL --input-format=Arrow -q "select count() from table"
$CLICKHOUSE_LOCAL -q "select * from numbers(10) format Arrow settings output_format_arrow_compression_method='lz4_frame'" | $CLICKHOUSE_LOCAL --input-format=Arrow -q "select count() from table"
$CLICKHOUSE_LOCAL -q "select * from numbers(10) format Arrow settings output_format_arrow_compression_method='zstd'" | $CLICKHOUSE_LOCAL --input-format=Arrow -q "select count() from table"

