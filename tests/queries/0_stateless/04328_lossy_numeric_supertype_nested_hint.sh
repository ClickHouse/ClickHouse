#!/usr/bin/env bash

# min/max/argMin/argMax and their combinators reject a Variant (top-level or nested inside
# Array/Map). The "illegal type" error must point at allow_lossy_numeric_supertype when enabling
# it would turn the Variant into a numeric supertype, and must stay silent when the setting cannot
# help. argMin/argMax check the compared value argument; the *ArgMin/*ArgMax combinators check the
# trailing key argument.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CH="${CLICKHOUSE_CLIENT} --use_variant_as_common_type=1 --allow_lossy_numeric_supertype=0"
ON="${CLICKHOUSE_CLIENT} --use_variant_as_common_type=1 --allow_lossy_numeric_supertype=1"
hint="allow_lossy_numeric_supertype"

# Presence check (1 = present, 0 = absent). The client may print the exception text more than once,
# so assert presence rather than counting occurrences.
has_hint() { grep -qF "$hint" && echo 1 || echo 0; }

# Negative path (1 = as expected). The query must still be rejected with ILLEGAL_TYPE_OF_ARGUMENT
# AND carry no setting hint. Requiring the error too means a query that starts succeeding (no error
# text) or that grows a hint flips the check to 0, instead of silently passing on absent hint alone.
throws_no_hint() { local out; out=$(cat); { echo "$out" | grep -qF "ILLEGAL_TYPE_OF_ARGUMENT" && ! echo "$out" | grep -qF "$hint"; } && echo 1 || echo 0; }

# Hint present (1): an all-numeric float-bearing Variant, top-level or nested in an Array element,
# a Map value, or a Map key.
$CH -q "SELECT min([toDecimal64(1, 2), 0.])" 2>&1 | has_hint
$CH -q "SELECT sum(if(materialize(1), toDecimal64(1, 2), 0.))" 2>&1 | has_hint
$CH -q "SELECT avg(multiIf(materialize(1), toInt64(1), 0.))" 2>&1 | has_hint
$CH -q "SELECT min(map('a', toDecimal64(1, 2), 'b', 0.))" 2>&1 | has_hint
$CH -q "SELECT min(map(toDecimal64(1, 2), 1, 0., 2))" 2>&1 | has_hint
# argMin/argMax check the compared value argument, top-level or nested in an Array via forEachChild.
$CH -q "SELECT argMin(1, if(materialize(1), toDecimal64(1, 2), 0.))" 2>&1 | has_hint
$CH -q "SELECT argMax(1, multiIf(materialize(1), toInt64(1), 0.))" 2>&1 | has_hint
$CH -q "SELECT argMin(1, [if(materialize(1), toDecimal64(1, 2), 0.)])" 2>&1 | has_hint
# The *ArgMin/*ArgMax combinators check the trailing key argument.
$CH -q "SELECT sumArgMin(materialize(toInt64(1)), if(materialize(1), toDecimal64(1, 2), 0.))" 2>&1 | has_hint

# Setting cannot help (1 = still rejected with ILLEGAL_TYPE_OF_ARGUMENT and no setting hint). An
# integer-only set (no float) stays a Variant; a non-numeric branch is not numeric; and composite
# branches resolve to a top-level Variant of the composites (Variant(Array, Array), not
# Array(Variant)), which the lossy fallback cannot re-resolve. Asserting the error too means these
# queries still have to be rejected, not merely lack the hint.
$CH -q "SELECT sum(if(materialize(toUInt8(1)), toInt64(1), toUInt64(2)))" 2>&1 | throws_no_hint
$CH -q "SELECT argMax(1, if(materialize(toUInt8(1)), toInt64(1), toUInt64(2)))" 2>&1 | throws_no_hint
$CH -q "SELECT avgArgMax(materialize(toInt64(1)), if(materialize(toUInt8(1)), toInt64(1), toUInt64(2)))" 2>&1 | throws_no_hint
$CH -q "SELECT min([toInt64(1), 'str'::String])" 2>&1 | throws_no_hint
$CH -q "SELECT min([toInt64(1), toUInt64(2)])" 2>&1 | throws_no_hint
$CH -q "SELECT min(if(materialize(1), [toDecimal64(1, 2)], [0.]))" 2>&1 | throws_no_hint
$CH -q "SELECT max(if(materialize(1), map('a', toDecimal64(1, 2)), map('a', 0.)))" 2>&1 | throws_no_hint

# Setting on: the nested numeric Variants resolve to numeric supertypes and aggregate.
$ON -q "SELECT min([toDecimal64(1, 2), 0.]), toTypeName([toDecimal64(1, 2), 0.])"
$ON -q "SELECT min(map('a', toDecimal64(1, 2), 'b', 0.)), toTypeName(map('a', toDecimal64(1, 2), 'b', 0.))"
$ON -q "SELECT min(map(toDecimal64(1, 2), 1, 0., 2)), toTypeName(map(toDecimal64(1, 2), 1, 0., 2))"
# A nullable numeric Map key resolves to Nullable(Float64), an invalid Map key, so it throws.
$ON -q "SELECT min(map(materialize(toNullable(toDecimal64(1, 2))), 1, 0., 2))" 2>&1 | grep -qF "Map cannot have a key of type" && echo 1 || echo 0
