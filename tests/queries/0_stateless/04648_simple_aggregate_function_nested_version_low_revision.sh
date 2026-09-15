#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# The announced `Native` type of a `SimpleAggregateFunction` whose value type holds a versioned
# `AggregateFunction` has to follow the peer's protocol revision, not only the negotiated version. A peer
# below `DBMS_MIN_REVISION_WITH_AGGREGATE_FUNCTIONS_VERSIONING` (54452) has no grammar for the
# `AggregateFunction(<version>, ...)` token, so announcing one leaves it unable to parse the header at all.
# Revision `0` is the local persister instead (`StripeLog`, `Set`, `Join`, a `FORMAT Native` file): it
# re-parses this very string, and an omitted version is read back as the function's default, so there the
# explicit `0` has to stay. The revision-0 round trip itself is covered by the `.sql` half of this pair.
#
# `Decimal32` is essential: `sumMap` v1 promotes it to `Decimal128`, so v0 and v1 payloads differ in width.

DECL="SimpleAggregateFunction(anyLast, AggregateFunction(0, sumMap, Array(UInt64), Array(Decimal32(2))))"
VALUE="sumMapState([1::UInt64, 2::UInt64], [10.5::Decimal32(2), 20.25::Decimal32(2)])"

# The same leaf declared *versionless* and placed under a `Tuple`, which pins the composite case. Here the
# token is announced at every revision, including the two below the threshold: a customized wrapper whose
# custom name cannot be rebuilt is left unreplaced (`transformTypesRecursively.cpp`), so the leaf never
# reaches the suppression above. That is the behaviour of the version walker rather than of this writer,
# and it is recorded here to keep it visible: the direct arm above moves with the threshold, this one
# does not, so a change to either walker or writer shows up as a diff in exactly one of them.
TUPLE_DECL="SimpleAggregateFunction(anyLast, Tuple(AggregateFunction(sumMap, Array(UInt64), Array(Decimal32(2)))))"
TUPLE_VALUE="tuple(sumMapState([1::UInt64, 2::UInt64], [10.5::Decimal32(2), 20.25::Decimal32(2)]))"

native_block() {
    ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&client_protocol_version=$1" \
        --data-binary "SELECT CAST($3, '$2') AS s FORMAT Native"
}

# The block is `num_columns`, `num_rows`, the length-prefixed column name, the length-prefixed type string,
# then the payload, so the type is the only printable run starting with `SimpleAggregateFunction(`. The
# `Tuple(` layer is optional so one pattern serves both declarations, and the `[^)]*` tail stops at the
# first `)` either way, which is past the position a version token would occupy.
TYPE_RUN='SimpleAggregateFunction\(anyLast, (Tuple\()?AggregateFunction\([^)]*'

for revision in 0 54450 54451 54452; do
    echo "revision $revision: $(native_block "$revision" "$DECL" "$VALUE" \
        | LC_ALL=C grep -aoE "$TYPE_RUN" | head -1)"
done

# Only the announced version moves: the payload is the trailing 25 bytes below the threshold, and it has to
# stay byte-identical to the revision-0 baseline so a payload regression cannot hide behind a correct string.
base=$(native_block 0 "$DECL" "$VALUE" | tail -c 25 | md5sum)
for revision in 54450 54451; do
    if [ "$(native_block "$revision" "$DECL" "$VALUE" | tail -c 25 | md5sum)" = "$base" ]; then
        echo "payload at $revision matches revision 0"
    else
        echo "payload at $revision DIFFERS from revision 0"
    fi
done

for revision in 0 54450 54451 54452; do
    echo "tuple revision $revision: $(native_block "$revision" "$TUPLE_DECL" "$TUPLE_VALUE" \
        | LC_ALL=C grep -aoE "$TYPE_RUN" | head -1)"
done

# 25 again, measured rather than carried over: a one-element `Tuple` writes no framing of its own, so this
# payload is exactly the direct arm's 25 bytes below the threshold.
tuple_base=$(native_block 0 "$TUPLE_DECL" "$TUPLE_VALUE" | tail -c 25 | md5sum)
for revision in 54450 54451; do
    if [ "$(native_block "$revision" "$TUPLE_DECL" "$TUPLE_VALUE" | tail -c 25 | md5sum)" = "$tuple_base" ]; then
        echo "tuple payload at $revision matches revision 0"
    else
        echo "tuple payload at $revision DIFFERS from revision 0"
    fi
done

# `groupBitmapAnd` negotiates version 1 only from 54455, so it is the one function whose token has to be
# emitted at a NON-ZERO revision: at 54452 and 54454 the live version is still 0 while the cached spelling
# re-parses to the default 1, so the explicit 0 must be announced there and dropped again at 54455. The
# `sumMap` arms above cannot pin that, because their own threshold is 54452 and their live version already
# agrees with the cached spelling from the first revision that permits a token at all.
BITMAP_DECL="SimpleAggregateFunction(anyLast, AggregateFunction(0, groupBitmapAnd, AggregateFunction(groupBitmap, UInt64)))"
BITMAP_VALUE="(SELECT groupBitmapAndState(z) FROM (SELECT groupBitmapState(u) AS z FROM (SELECT 42::UInt64 AS u)))"
BITMAP_RUN='SimpleAggregateFunction\(anyLast, AggregateFunction\([^)]*'

for revision in 0 54451 54452 54454 54455; do
    echo "bitmap revision $revision: $(native_block "$revision" "$BITMAP_DECL" "$BITMAP_VALUE" \
        | LC_ALL=C grep -aoE "$BITMAP_RUN" | head -1)"
done
