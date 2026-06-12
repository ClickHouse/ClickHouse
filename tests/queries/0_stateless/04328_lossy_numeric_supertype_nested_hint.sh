#!/usr/bin/env bash

# min/max reject a Variant nested inside Array/Map. The "illegal type" error must still point at
# allow_lossy_numeric_supertype when enabling it would turn the nested Variant into a numeric
# supertype (e.g. Array(Float64)), and must stay silent when the setting cannot help.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CH="${CLICKHOUSE_CLIENT} --use_variant_as_common_type=1 --allow_lossy_numeric_supertype=0"
hint="allow_lossy_numeric_supertype"

# Setting off: the hint must be present for all-numeric Variants nested in Array/Map.
$CH -q "SELECT min([toDecimal64(1, 2), 0.])" 2>&1 | grep -oF "$hint" | head -n1
$CH -q "SELECT max([toDecimal64(1, 2), 0.])" 2>&1 | grep -oF "$hint" | head -n1
$CH -q "SELECT min(map('a', toDecimal64(1, 2), 'b', 0.))" 2>&1 | grep -oF "$hint" | head -n1

# No hint when the setting cannot help: a non-numeric branch, or an integer-only set (no float).
$CH -q "SELECT min([toInt64(1), 'str'::String])" 2>&1 | grep -cF "$hint"
$CH -q "SELECT min([toInt64(1), toUInt64(2)])" 2>&1 | grep -cF "$hint"

# Setting on: the nested numeric supertype is used, so the aggregate works.
${CLICKHOUSE_CLIENT} --use_variant_as_common_type=1 --allow_lossy_numeric_supertype=1 -q "SELECT min([toDecimal64(1, 2), 0.]), toTypeName([toDecimal64(1, 2), 0.])"
${CLICKHOUSE_CLIENT} --use_variant_as_common_type=1 --allow_lossy_numeric_supertype=1 -q "SELECT min(map('a', toDecimal64(1, 2), 'b', 0.)), toTypeName(map('a', toDecimal64(1, 2), 'b', 0.))"
