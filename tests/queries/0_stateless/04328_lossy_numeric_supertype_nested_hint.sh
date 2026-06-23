#!/usr/bin/env bash

# min/max reject a Variant nested inside Array/Map. The "illegal type" error must still point at
# allow_lossy_numeric_supertype when enabling it would turn the nested Variant into a numeric
# supertype (e.g. Array(Float64)), and must stay silent when the setting cannot help.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CH="${CLICKHOUSE_CLIENT} --use_variant_as_common_type=1 --allow_lossy_numeric_supertype=0"
hint="allow_lossy_numeric_supertype"
# The general hint (if/multiIf/coalesce/array, Array element, Map value) promises the setting
# resolves the Variant. The Map-key hint is worded differently because a Map key cannot be Nullable.
general="if/multiIf/coalesce/array"
mapkey="numeric map key"

# Setting off: the general hint must be present for all-numeric Variants nested in Array and Map value.
$CH -q "SELECT min([toDecimal64(1, 2), 0.])" 2>&1 | grep -oF "$general" | head -n1
$CH -q "SELECT max([toDecimal64(1, 2), 0.])" 2>&1 | grep -oF "$general" | head -n1
$CH -q "SELECT min(map('a', toDecimal64(1, 2), 'b', 0.))" 2>&1 | grep -oF "$general" | head -n1

# sum/avg also append the hint for a scalar top-level numeric Variant. 04327 only checks the error
# code for these, so assert the hint text here: present for a float-bearing Variant, absent for an
# integer-only one (the setting cannot help, so it must not be suggested).
$CH -q "SELECT sum(if(materialize(1), toDecimal64(1, 2), 0.))" 2>&1 | grep -oF "$general" | head -n1
$CH -q "SELECT avg(multiIf(materialize(1), toInt64(1), 0.))" 2>&1 | grep -oF "$general" | head -n1
$CH -q "SELECT sum(if(materialize(toUInt8(1)), toInt64(1), toUInt64(2)))" 2>&1 | grep -cF "$hint"

# No hint when the setting cannot help: a non-numeric branch, or an integer-only set (no float).
$CH -q "SELECT min([toInt64(1), 'str'::String])" 2>&1 | grep -cF "$hint"
$CH -q "SELECT min([toInt64(1), toUInt64(2)])" 2>&1 | grep -cF "$hint"

# if/multiIf/coalesce over composite branches resolve to a top-level Variant of the composites
# (Variant(Array, Array), not Array(Variant(...))), which the lossy fallback cannot re-resolve: it
# only retries top-level branch types and the Variant's alternatives are not numeric. The hint must
# stay silent here, otherwise it would point users at a setting that does not help.
$CH -q "SELECT min(if(materialize(1), [toDecimal64(1, 2)], [0.]))" 2>&1 | grep -cF "$hint"
$CH -q "SELECT min(multiIf(materialize(1), [toDecimal64(1, 2)], materialize(2), [toInt64(3)], [0.]))" 2>&1 | grep -cF "$hint"
$CH -q "SELECT max(if(materialize(1), map('a', toDecimal64(1, 2)), map('a', 0.)))" 2>&1 | grep -cF "$hint"
# The setting genuinely cannot help: the branch stays a Variant even with it on, so the silence above is correct.
${CLICKHOUSE_CLIENT} --use_variant_as_common_type=1 --allow_lossy_numeric_supertype=1 -q "SELECT toTypeName(if(materialize(1), [toDecimal64(1, 2)], [0.]))"

# Numeric Variant Map key: a Map key cannot be Nullable, so the resolved type is identical for a
# nullable and a non-nullable key. The setting resolves only a non-nullable key (FunctionMap maps it
# to Float64); a nullable key stays a Variant. min/max cannot tell the two apart here, so the Map-key
# hint names the setting without claiming it always works. It must be the Map-key wording, not the
# general one. Covers the non-nullable key, the nullable key, and the key nested inside an Array.
$CH -q "SELECT min(map(toDecimal64(1, 2), 1, 0., 2))" 2>&1 | grep -oF "$mapkey" | head -n1
$CH -q "SELECT min(map(materialize(toNullable(toDecimal64(1, 2))), 1, 0., 2))" 2>&1 | grep -oF "$mapkey" | head -n1
$CH -q "SELECT max(map(materialize(toNullable(toDecimal64(1, 2))), 1, 0., 2))" 2>&1 | grep -oF "$mapkey" | head -n1
$CH -q "SELECT min([map(materialize(toNullable(toDecimal64(1, 2))), 1, 0., 2)])" 2>&1 | grep -oF "$mapkey" | head -n1
# The Map-key hint must not use the general wording, and the general sites must not use the Map-key wording.
$CH -q "SELECT min(map(toDecimal64(1, 2), 1, 0., 2))" 2>&1 | grep -cF "$general"
$CH -q "SELECT min(map('a', toDecimal64(1, 2), 'b', 0.))" 2>&1 | grep -cF "$mapkey"

# Setting on: a non-nullable numeric Map key resolves to Float64 and the aggregate works (the
# Map-key hint was truthful). A nullable Map key keeps the Variant, so it still throws.
${CLICKHOUSE_CLIENT} --use_variant_as_common_type=1 --allow_lossy_numeric_supertype=1 -q "SELECT min([toDecimal64(1, 2), 0.]), toTypeName([toDecimal64(1, 2), 0.])"
${CLICKHOUSE_CLIENT} --use_variant_as_common_type=1 --allow_lossy_numeric_supertype=1 -q "SELECT min(map('a', toDecimal64(1, 2), 'b', 0.)), toTypeName(map('a', toDecimal64(1, 2), 'b', 0.))"
${CLICKHOUSE_CLIENT} --use_variant_as_common_type=1 --allow_lossy_numeric_supertype=1 -q "SELECT min(map(toDecimal64(1, 2), 1, 0., 2)), toTypeName(map(toDecimal64(1, 2), 1, 0., 2))"
