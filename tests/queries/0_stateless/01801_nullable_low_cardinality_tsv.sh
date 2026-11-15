#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS nullable_low_cardinality_tsv_test;";
$CLICKHOUSE_CLIENT --query="CREATE TABLE nullable_low_cardinality_tsv_test
(
    A Date,
    S LowCardinality(Nullable(String)),
    X Int32,
    S1 LowCardinality(Nullable(String)),
    S2 Array(String)
) ENGINE=TinyLog";

printf '2020-01-01\t\N\t32\t\N\n' | $CLICKHOUSE_CLIENT -q 'insert into nullable_low_cardinality_tsv_test format TSV' 2>&1 \
    | grep -q "Code: 27"

echo $?;

$CLICKHOUSE_CLIENT --query="DROP TABLE nullable_low_cardinality_tsv_test";
