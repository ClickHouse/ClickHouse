#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_LOCAL -q "select * from file('$CURDIR/data_npy/one_dim.npy')"
$CLICKHOUSE_LOCAL -q "select * from file('$CURDIR/data_npy/one_dim_float.npy')"
$CLICKHOUSE_LOCAL -q "select * from file('$CURDIR/data_npy/one_dim_str.npy')"
$CLICKHOUSE_LOCAL -q "select * from file('$CURDIR/data_npy/one_dim_unicode.npy')"
$CLICKHOUSE_LOCAL -q "select * from file('$CURDIR/data_npy/two_dim.npy')"
$CLICKHOUSE_LOCAL -q "select * from file('$CURDIR/data_npy/two_dim_float.npy')"
$CLICKHOUSE_LOCAL -q "select * from file('$CURDIR/data_npy/two_dim_str.npy')"
$CLICKHOUSE_LOCAL -q "select * from file('$CURDIR/data_npy/two_dim_unicode.npy')"
$CLICKHOUSE_LOCAL -q "select * from file('$CURDIR/data_npy/two_dim_bool.npy')"
$CLICKHOUSE_LOCAL -q "select * from file('$CURDIR/data_npy/two_dim_null.npy')"
$CLICKHOUSE_LOCAL -q "select * from file('$CURDIR/data_npy/three_dim.npy')"
