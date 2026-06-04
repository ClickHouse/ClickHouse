#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_LOCAL -q "select count() from file('$CURDIR/data_npy/one_dim.npy') settings optimize_count_from_files=0"
$CLICKHOUSE_LOCAL -q "select count() from file('$CURDIR/data_npy/one_dim.npy') settings optimize_count_from_files=1"
$CLICKHOUSE_LOCAL -q "select count() from file('$CURDIR/data_npy/one_dim.npy', auto, 'array Int64') settings optimize_count_from_files=1"
$CLICKHOUSE_LOCAL -m -q "
desc file('$CURDIR/data_npy/one_dim.npy');
select number_of_rows from system.schema_inference_cache where format='Npy';
"
$CLICKHOUSE_LOCAL -q "select count() from file('$CURDIR/data_npy/npy_big.npy') settings optimize_count_from_files=0"
$CLICKHOUSE_LOCAL -q "select count() from file('$CURDIR/data_npy/npy_big.npy') settings optimize_count_from_files=1"
$CLICKHOUSE_LOCAL -m -q "
desc file('$CURDIR/data_npy/npy_big.npy');
select number_of_rows from system.schema_inference_cache where format='Npy';
"
