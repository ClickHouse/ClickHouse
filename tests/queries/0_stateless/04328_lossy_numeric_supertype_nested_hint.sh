#!/usr/bin/env bash

# min/max reject a Variant nested inside Array/Map. The "illegal type" error must point at
# allow_lossy_numeric_supertype when enabling it would turn the nested Variant into a numeric
# supertype, and must stay silent when the setting cannot help.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CH="${CLICKHOUSE_CLIENT} --use_variant_as_common_type=1 --allow_lossy_numeric_supertype=0"
ON="${CLICKHOUSE_CLIENT} --use_variant_as_common_type=1 --allow_lossy_numeric_supertype=1"
hint="allow_lossy_numeric_supertype"

# Presence check (1 = present, 0 = absent). The client may print the exception text more than once,
# so assert presence rather than counting occurrences.
has_hint() { grep -qF "$hint" && echo 1 || echo 0; }

# Hint present (1): an all-numeric float-bearing Variant, top-level or nested in an Array element,
# a Map value, or a Map key.
$CH -q "SELECT min([toDecimal64(1, 2), 0.])" 2>&1 | has_hint
$CH -q "SELECT sum(if(materialize(1), toDecimal64(1, 2), 0.))" 2>&1 | has_hint
$CH -q "SELECT avg(multiIf(materialize(1), toInt64(1), 0.))" 2>&1 | has_hint
$CH -q "SELECT min(map('a', toDecimal64(1, 2), 'b', 0.))" 2>&1 | has_hint
$CH -q "SELECT min(map(toDecimal64(1, 2), 1, 0., 2))" 2>&1 | has_hint

# Hint absent (0): the setting cannot help. An integer-only set (no float) stays a Variant; a
# non-numeric branch is not numeric; and composite branches resolve to a top-level Variant of the
# composites (Variant(Array, Array), not Array(Variant)), which the lossy fallback cannot re-resolve.
$CH -q "SELECT sum(if(materialize(toUInt8(1)), toInt64(1), toUInt64(2)))" 2>&1 | has_hint
$CH -q "SELECT min([toInt64(1), 'str'::String])" 2>&1 | has_hint
$CH -q "SELECT min([toInt64(1), toUInt64(2)])" 2>&1 | has_hint
$CH -q "SELECT min(if(materialize(1), [toDecimal64(1, 2)], [0.]))" 2>&1 | has_hint
$CH -q "SELECT max(if(materialize(1), map('a', toDecimal64(1, 2)), map('a', 0.)))" 2>&1 | has_hint

# Setting on: the nested numeric Variants resolve to numeric supertypes and aggregate.
$ON -q "SELECT min([toDecimal64(1, 2), 0.]), toTypeName([toDecimal64(1, 2), 0.])"
$ON -q "SELECT min(map('a', toDecimal64(1, 2), 'b', 0.)), toTypeName(map('a', toDecimal64(1, 2), 'b', 0.))"
$ON -q "SELECT min(map(toDecimal64(1, 2), 1, 0., 2)), toTypeName(map(toDecimal64(1, 2), 1, 0., 2))"
# A nullable numeric Map key resolves to Nullable(Float64), an invalid Map key, so it throws.
$ON -q "SELECT min(map(materialize(toNullable(toDecimal64(1, 2))), 1, 0., 2))" 2>&1 | grep -qF "Map cannot have a key of type" && echo 1 || echo 0
