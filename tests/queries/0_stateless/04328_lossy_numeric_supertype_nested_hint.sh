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

# No hint for a numeric Variant Map key: a Map key cannot be Nullable, so FunctionMap keeps the
# plain Variant key even with the setting on, and the aggregate would still fail. Covers the
# nullable-numeric key directly and nested inside an Array.
$CH -q "SELECT min(map(materialize(toNullable(toDecimal64(1, 2))), 1, 0., 2))" 2>&1 | grep -cF "$hint"
$CH -q "SELECT max(map(materialize(toNullable(toDecimal64(1, 2))), 1, 0., 2))" 2>&1 | grep -cF "$hint"
$CH -q "SELECT min([map(materialize(toNullable(toDecimal64(1, 2))), 1, 0., 2)])" 2>&1 | grep -cF "$hint"
# A Map with a numeric Variant in both key and value: the key Variant survives, so even though the
# value would become Float64 the aggregate still fails. The hint must stay silent.
$CH -q "SELECT min(map(materialize(toNullable(toDecimal64(1, 2))), materialize(toNullable(toDecimal64(3, 2))), 0., 1.5))" 2>&1 | grep -cF "$hint"

# Setting on: the nested numeric supertype is used, so the aggregate works.
${CLICKHOUSE_CLIENT} --use_variant_as_common_type=1 --allow_lossy_numeric_supertype=1 -q "SELECT min([toDecimal64(1, 2), 0.]), toTypeName([toDecimal64(1, 2), 0.])"
${CLICKHOUSE_CLIENT} --use_variant_as_common_type=1 --allow_lossy_numeric_supertype=1 -q "SELECT min(map('a', toDecimal64(1, 2), 'b', 0.)), toTypeName(map('a', toDecimal64(1, 2), 'b', 0.))"
