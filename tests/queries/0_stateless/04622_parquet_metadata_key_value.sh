#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_LOCAL -q "select key_value_metadata from file('$CURDIR/data_parquet/ipv6_bloom_filter.gz.parquet', ParquetMetadata)"

$CLICKHOUSE_LOCAL -q "select mapKeys(key_value_metadata), key_value_metadata['writer.model.name'] from file('$CURDIR/data_parquet/ipv6_bloom_filter.gz.parquet', ParquetMetadata)"

$CLICKHOUSE_LOCAL -q "select key_value_metadata from file('$CURDIR/data_parquet/02718_data.parquet', ParquetMetadata)"
