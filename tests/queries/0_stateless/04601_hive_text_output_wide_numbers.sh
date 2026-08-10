#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Hive has no numeric type covering the 128-bit and 256-bit integer domains: its widest integer is
# BIGINT (64-bit), and even Hive DECIMAL with its maximum precision of 38 cannot hold the whole
# Int128 range. These types must throw NOT_IMPLEMENTED for HiveText output instead of writing
# values that no Hive schema could read back.
for expr in \
    "toInt128(1)" \
    "toUInt128(1)" \
    "toInt256(1)" \
    "toUInt256(1)"
do
    ${CLICKHOUSE_CLIENT} --query "SELECT ${expr} FORMAT HiveText" 2>&1 | grep -o -m1 "Type [A-Za-z0-9]* is not supported by the HiveText output format"
done

# The same applies to Decimal precisions above 38, the maximum of Hive DECIMAL.
for expr in \
    "toDecimal256(1, 0)" \
    "CAST(1, 'Decimal(39, 2)')"
do
    ${CLICKHOUSE_CLIENT} --query "SELECT ${expr} FORMAT HiveText" 2>&1 | grep -o -m1 "Decimal precision [0-9]* is not supported by the HiveText output format"
done

# Numeric types whose values fit into Hive numeric types remain supported, including the maximum
# Hive DECIMAL precision of 38.
${CLICKHOUSE_CLIENT} --query "SELECT toUInt64(18446744073709551615), toInt64(-9223372036854775808), toUInt8(255), toFloat32(0.5), toFloat64(-0.25), toDecimal32('1.2', 1), toDecimal64('3.4', 2), toDecimal128('5.6', 3), CAST('99999999999999999999999999999999999999', 'Decimal(38, 0)') FORMAT HiveText"
