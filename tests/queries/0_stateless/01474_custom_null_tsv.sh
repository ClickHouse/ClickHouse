#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS tsv_custom_null";
$CLICKHOUSE_CLIENT --query="CREATE TABLE tsv_custom_null (id Nullable(UInt32)) ENGINE = Memory";

$CLICKHOUSE_CLIENT --query="INSERT INTO tsv_custom_null VALUES (NULL)";

$CLICKHOUSE_CLIENT --output_format_tsv_null_representation='MyNull' --query="SELECT * FROM tsv_custom_null FORMAT TSV";

$CLICKHOUSE_CLIENT --query="DROP TABLE tsv_custom_null";

