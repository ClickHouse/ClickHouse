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
$CLICKHOUSE_LOCAL -q "select * from file('$CURDIR/data_npy/none_endian_array.npy')"
$CLICKHOUSE_LOCAL -q "select * from file('$CURDIR/data_npy/big_endian_array.npy')"

$CLICKHOUSE_LOCAL -q "describe file('$CURDIR/data_npy/one_dim.npy')"
$CLICKHOUSE_LOCAL -q "describe file('$CURDIR/data_npy/one_dim_float.npy')"
$CLICKHOUSE_LOCAL -q "describe file('$CURDIR/data_npy/one_dim_str.npy')"
$CLICKHOUSE_LOCAL -q "describe file('$CURDIR/data_npy/one_dim_unicode.npy')"
$CLICKHOUSE_LOCAL -q "describe file('$CURDIR/data_npy/two_dim.npy')"
$CLICKHOUSE_LOCAL -q "describe file('$CURDIR/data_npy/two_dim_float.npy')"
$CLICKHOUSE_LOCAL -q "describe file('$CURDIR/data_npy/two_dim_str.npy')"
$CLICKHOUSE_LOCAL -q "describe file('$CURDIR/data_npy/two_dim_unicode.npy')"
$CLICKHOUSE_LOCAL -q "describe file('$CURDIR/data_npy/two_dim_bool.npy')"
$CLICKHOUSE_LOCAL -q "describe file('$CURDIR/data_npy/two_dim_null.npy')"
$CLICKHOUSE_LOCAL -q "describe file('$CURDIR/data_npy/three_dim.npy')"
$CLICKHOUSE_LOCAL -q "describe file('$CURDIR/data_npy/none_endian_array.npy')"
$CLICKHOUSE_LOCAL -q "describe file('$CURDIR/data_npy/big_endian_array.npy')"

$CLICKHOUSE_LOCAL -q "select * from file('$CURDIR/data_npy/one_dim.npy', Npy, 'value UInt8')"
$CLICKHOUSE_LOCAL -q "select * from file('$CURDIR/data_npy/one_dim.npy', Npy, 'value UInt16')"
$CLICKHOUSE_LOCAL -q "select * from file('$CURDIR/data_npy/one_dim.npy', Npy, 'value UInt32')"
$CLICKHOUSE_LOCAL -q "select * from file('$CURDIR/data_npy/one_dim.npy', Npy, 'value UInt64')"
$CLICKHOUSE_LOCAL -q "select * from file('$CURDIR/data_npy/one_dim.npy', Npy, 'value Int8')"
$CLICKHOUSE_LOCAL -q "select * from file('$CURDIR/data_npy/one_dim.npy', Npy, 'value Int16')"
$CLICKHOUSE_LOCAL -q "select * from file('$CURDIR/data_npy/one_dim.npy', Npy, 'value Int32')"
$CLICKHOUSE_LOCAL -q "select * from file('$CURDIR/data_npy/one_dim.npy', Npy, 'value Int64')"

$CLICKHOUSE_LOCAL -q "select * from file('$CURDIR/data_npy/one_dim_float.npy', Npy, 'value Float32')"
$CLICKHOUSE_LOCAL -q "select * from file('$CURDIR/data_npy/one_dim_float.npy', Npy, 'value Float64')"

$CLICKHOUSE_LOCAL -q "select * from file('$CURDIR/data_npy/one_dim_str.npy', Npy, 'value FixedString(1)')"
$CLICKHOUSE_LOCAL -q "select * from file('$CURDIR/data_npy/one_dim_str.npy', Npy, 'value String')"

$CLICKHOUSE_LOCAL -q "select * from file('$CURDIR/data_npy/two_dim.npy', Npy, 'value Array(Int8)')"
$CLICKHOUSE_LOCAL -q "select * from file('$CURDIR/data_npy/three_dim.npy', Npy, 'value Array(Array(Int8))')"

$CLICKHOUSE_LOCAL -q "select * from file('$CURDIR/data_npy/one_dim_float.npy', Npy, 'value Array(Float32)')" 2>&1 | grep -c "BAD_ARGUMENTS"
$CLICKHOUSE_LOCAL -q "select * from file('$CURDIR/data_npy/one_dim_float.npy', Npy, 'value UUID')" 2>&1 | grep -c "UNKNOWN_TYPE"
$CLICKHOUSE_LOCAL -q "select * from file('$CURDIR/data_npy/one_dim_float.npy', Npy, 'value Tuple(UInt8)')" 2>&1 | grep -c "UNKNOWN_TYPE"

$CLICKHOUSE_LOCAL -q "select * from file('$CURDIR/data_npy/one_dim_float.npy', Npy, 'value Int8')" 2>&1 | grep -c "ILLEGAL_COLUMN"
$CLICKHOUSE_LOCAL -q "select * from file('$CURDIR/data_npy/one_dim_str.npy', Npy, 'value Int8')" 2>&1 | grep -c "ILLEGAL_COLUMN"
$CLICKHOUSE_LOCAL -q "select * from file('$CURDIR/data_npy/one_dim_unicode.npy', Npy, 'value Float32')" 2>&1 | grep -c "ILLEGAL_COLUMN"

$CLICKHOUSE_LOCAL -q "select * from file('$CURDIR/data_npy/complex.npy')" 2>&1 | grep -c "CANNOT_EXTRACT_TABLE_STRUCTURE"

$CLICKHOUSE_LOCAL -q "select * from file('$CURDIR/data_npy/float_16.npy')"

$CLICKHOUSE_LOCAL -q "select * from file('$CURDIR/data_npy/npy_inf_nan_null.npy')"
