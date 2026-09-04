-- Covers rb_and_cardinality and rb_intersect in
-- AggregateFunctions/AggregateFunctionGroupBitmapData.h across every combination of
-- small-set and Roaring representation, including the Large x Small operand order.
--
-- bitmapAndCardinality and bitmapHasAny are cross-checked against bitmapAnd +
-- bitmapCardinality, which reaches the answer through a different code path
-- (rb_and followed by size()). Every assertion below prints 1 when the two agree.
--
-- Operands of 40 elements exceed the small_set_size = 32 promotion threshold, so
-- they are held as Roaring bitmaps; 3-element operands stay in the small set.

SELECT '--- UInt32: Small x Small ---';
WITH
    bitmapBuild([1, 5, 7]::Array(UInt32)) AS a,
    bitmapBuild([5, 7, 99]::Array(UInt32)) AS b
SELECT
    bitmapAndCardinality(a, b) = bitmapCardinality(bitmapAnd(a, b)),
    bitmapOrCardinality(a, b) = bitmapCardinality(bitmapOr(a, b)),
    bitmapXorCardinality(a, b) = bitmapCardinality(bitmapXor(a, b)),
    bitmapAndnotCardinality(a, b) = bitmapCardinality(bitmapAndnot(a, b)),
    bitmapHasAny(a, b) = (bitmapCardinality(bitmapAnd(a, b)) > 0);

SELECT '--- UInt32: Small x Large ---';
WITH
    bitmapBuild([1, 5, 7]::Array(UInt32)) AS a,
    bitmapBuild(arrayMap(x -> toUInt32(x), range(40))) AS b
SELECT
    bitmapAndCardinality(a, b) = bitmapCardinality(bitmapAnd(a, b)),
    bitmapOrCardinality(a, b) = bitmapCardinality(bitmapOr(a, b)),
    bitmapXorCardinality(a, b) = bitmapCardinality(bitmapXor(a, b)),
    bitmapAndnotCardinality(a, b) = bitmapCardinality(bitmapAndnot(a, b)),
    bitmapHasAny(a, b) = (bitmapCardinality(bitmapAnd(a, b)) > 0);

SELECT '--- UInt32: Large x Small ---';
WITH
    bitmapBuild(arrayMap(x -> toUInt32(x), range(40))) AS a,
    bitmapBuild([1, 5, 7]::Array(UInt32)) AS b
SELECT
    bitmapAndCardinality(a, b) = bitmapCardinality(bitmapAnd(a, b)),
    bitmapOrCardinality(a, b) = bitmapCardinality(bitmapOr(a, b)),
    bitmapXorCardinality(a, b) = bitmapCardinality(bitmapXor(a, b)),
    bitmapAndnotCardinality(a, b) = bitmapCardinality(bitmapAndnot(a, b)),
    bitmapHasAny(a, b) = (bitmapCardinality(bitmapAnd(a, b)) > 0);

SELECT '--- UInt32: Large x Small, no shared element ---';
WITH
    bitmapBuild(arrayMap(x -> toUInt32(x), range(40))) AS a,
    bitmapBuild([100, 500, 700]::Array(UInt32)) AS b
SELECT
    bitmapAndCardinality(a, b) = bitmapCardinality(bitmapAnd(a, b)),
    bitmapOrCardinality(a, b) = bitmapCardinality(bitmapOr(a, b)),
    bitmapXorCardinality(a, b) = bitmapCardinality(bitmapXor(a, b)),
    bitmapAndnotCardinality(a, b) = bitmapCardinality(bitmapAndnot(a, b)),
    bitmapHasAny(a, b) = (bitmapCardinality(bitmapAnd(a, b)) > 0);

SELECT '--- UInt32: Large x Large, overlapping ---';
WITH
    bitmapBuild(arrayMap(x -> toUInt32(x), range(40))) AS a,
    bitmapBuild(arrayMap(x -> toUInt32(x + 20), range(40))) AS b
SELECT
    bitmapAndCardinality(a, b) = bitmapCardinality(bitmapAnd(a, b)),
    bitmapOrCardinality(a, b) = bitmapCardinality(bitmapOr(a, b)),
    bitmapXorCardinality(a, b) = bitmapCardinality(bitmapXor(a, b)),
    bitmapAndnotCardinality(a, b) = bitmapCardinality(bitmapAndnot(a, b)),
    bitmapHasAny(a, b) = (bitmapCardinality(bitmapAnd(a, b)) > 0);

SELECT '--- UInt32: Large x Large, disjoint ---';
WITH
    bitmapBuild(arrayMap(x -> toUInt32(x), range(40))) AS a,
    bitmapBuild(arrayMap(x -> toUInt32(x + 1000), range(40))) AS b
SELECT
    bitmapAndCardinality(a, b) = bitmapCardinality(bitmapAnd(a, b)),
    bitmapOrCardinality(a, b) = bitmapCardinality(bitmapOr(a, b)),
    bitmapXorCardinality(a, b) = bitmapCardinality(bitmapXor(a, b)),
    bitmapAndnotCardinality(a, b) = bitmapCardinality(bitmapAndnot(a, b)),
    bitmapHasAny(a, b) = (bitmapCardinality(bitmapAnd(a, b)) > 0);

-- Elements spaced 70000 apart land in distinct 16-bit Roaring containers, so these
-- two cases walk many container pairs rather than a single one.

SELECT '--- UInt32: multi-container, shared element ---';
WITH
    bitmapBuild(arrayMap(x -> toUInt32(x * 70000), range(40))) AS a,
    bitmapBuild(arrayConcat([toUInt32(0)], arrayMap(x -> toUInt32(x * 70000 + 1), range(40)))) AS b
SELECT
    bitmapAndCardinality(a, b) = bitmapCardinality(bitmapAnd(a, b)),
    bitmapOrCardinality(a, b) = bitmapCardinality(bitmapOr(a, b)),
    bitmapXorCardinality(a, b) = bitmapCardinality(bitmapXor(a, b)),
    bitmapAndnotCardinality(a, b) = bitmapCardinality(bitmapAndnot(a, b)),
    bitmapHasAny(a, b) = (bitmapCardinality(bitmapAnd(a, b)) > 0);

-- Same container keys on both sides but no shared element, so bitmapHasAny cannot
-- exit early and has to examine every container pair.

SELECT '--- UInt32: multi-container, no shared element ---';
WITH
    bitmapBuild(arrayMap(x -> toUInt32(x * 70000), range(40))) AS a,
    bitmapBuild(arrayMap(x -> toUInt32(x * 70000 + 1), range(40))) AS b
SELECT
    bitmapAndCardinality(a, b) = bitmapCardinality(bitmapAnd(a, b)),
    bitmapOrCardinality(a, b) = bitmapCardinality(bitmapOr(a, b)),
    bitmapXorCardinality(a, b) = bitmapCardinality(bitmapXor(a, b)),
    bitmapAndnotCardinality(a, b) = bitmapCardinality(bitmapAndnot(a, b)),
    bitmapHasAny(a, b) = (bitmapCardinality(bitmapAnd(a, b)) > 0);

-- bitmapAndnotCardinality is deliberately absent: it is not commutative.

SELECT '--- UInt32: operand order, Large x Small vs Small x Large ---';
WITH
    bitmapBuild(arrayMap(x -> toUInt32(x), range(40))) AS a,
    bitmapBuild([1, 5, 7]::Array(UInt32)) AS b
SELECT
    bitmapAndCardinality(a, b) = bitmapAndCardinality(b, a),
    bitmapOrCardinality(a, b) = bitmapOrCardinality(b, a),
    bitmapXorCardinality(a, b) = bitmapXorCardinality(b, a),
    bitmapHasAny(a, b) = bitmapHasAny(b, a);

SELECT '--- UInt32: operand order, Large x Large ---';
WITH
    bitmapBuild(arrayMap(x -> toUInt32(x), range(40))) AS a,
    bitmapBuild(arrayMap(x -> toUInt32(x + 20), range(40))) AS b
SELECT
    bitmapAndCardinality(a, b) = bitmapAndCardinality(b, a),
    bitmapOrCardinality(a, b) = bitmapOrCardinality(b, a),
    bitmapXorCardinality(a, b) = bitmapXorCardinality(b, a),
    bitmapHasAny(a, b) = bitmapHasAny(b, a);

-- Pins the arithmetic to fixed expectations rather than only to the reference path.
-- a = {0..39}, b = {20..59}, so |a & b| = 20, |a | b| = 60, |a ^ b| = 40, |a \ b| = 20.

SELECT '--- UInt32: exact cardinalities, Large x Large ---';
WITH
    bitmapBuild(arrayMap(x -> toUInt32(x), range(40))) AS a,
    bitmapBuild(arrayMap(x -> toUInt32(x + 20), range(40))) AS b
SELECT
    bitmapAndCardinality(a, b) = 20,
    bitmapOrCardinality(a, b) = 60,
    bitmapXorCardinality(a, b) = 40,
    bitmapAndnotCardinality(a, b) = 20,
    bitmapHasAny(a, b) = 1;

-- UInt64 operands are backed by Roaring64Map, which has no direct and_cardinality or
-- intersect, so these cases exercise the fallback rather than the fast path.

SELECT '--- UInt64: Large x Large, overlapping ---';
WITH
    bitmapBuild(arrayMap(x -> toUInt64(x), range(40))) AS a,
    bitmapBuild(arrayMap(x -> toUInt64(x + 20), range(40))) AS b
SELECT
    bitmapAndCardinality(a, b) = bitmapCardinality(bitmapAnd(a, b)),
    bitmapOrCardinality(a, b) = bitmapCardinality(bitmapOr(a, b)),
    bitmapXorCardinality(a, b) = bitmapCardinality(bitmapXor(a, b)),
    bitmapAndnotCardinality(a, b) = bitmapCardinality(bitmapAndnot(a, b)),
    bitmapHasAny(a, b) = (bitmapCardinality(bitmapAnd(a, b)) > 0);

SELECT '--- UInt64: Large x Small ---';
WITH
    bitmapBuild(arrayMap(x -> toUInt64(x), range(40))) AS a,
    bitmapBuild([1, 5, 7]::Array(UInt64)) AS b
SELECT
    bitmapAndCardinality(a, b) = bitmapCardinality(bitmapAnd(a, b)),
    bitmapOrCardinality(a, b) = bitmapCardinality(bitmapOr(a, b)),
    bitmapXorCardinality(a, b) = bitmapCardinality(bitmapXor(a, b)),
    bitmapAndnotCardinality(a, b) = bitmapCardinality(bitmapAndnot(a, b)),
    bitmapHasAny(a, b) = (bitmapCardinality(bitmapAnd(a, b)) > 0);

SELECT '--- UInt64: above the 32-bit range ---';
WITH
    bitmapBuild(arrayMap(x -> toUInt64(x + 4294967296), range(40))) AS a,
    bitmapBuild(arrayMap(x -> toUInt64(x + 4294967316), range(40))) AS b
SELECT
    bitmapAndCardinality(a, b) = bitmapCardinality(bitmapAnd(a, b)),
    bitmapOrCardinality(a, b) = bitmapCardinality(bitmapOr(a, b)),
    bitmapXorCardinality(a, b) = bitmapCardinality(bitmapXor(a, b)),
    bitmapAndnotCardinality(a, b) = bitmapCardinality(bitmapAndnot(a, b)),
    bitmapHasAny(a, b) = (bitmapCardinality(bitmapAnd(a, b)) > 0);

SELECT '--- UInt64: operand order, Large x Small ---';
WITH
    bitmapBuild(arrayMap(x -> toUInt64(x), range(40))) AS a,
    bitmapBuild([1, 5, 7]::Array(UInt64)) AS b
SELECT
    bitmapAndCardinality(a, b) = bitmapAndCardinality(b, a),
    bitmapOrCardinality(a, b) = bitmapOrCardinality(b, a),
    bitmapXorCardinality(a, b) = bitmapXorCardinality(b, a),
    bitmapHasAny(a, b) = bitmapHasAny(b, a);

-- Int32 and Int16 reach the same 32-bit fast path through a different type dispatch.

SELECT '--- Int32 / Int16: Large x Large ---';
WITH
    bitmapBuild(arrayMap(x -> toInt32(x), range(40))) AS a,
    bitmapBuild(arrayMap(x -> toInt32(x + 20), range(40))) AS b
SELECT
    bitmapAndCardinality(a, b) = bitmapCardinality(bitmapAnd(a, b)),
    bitmapHasAny(a, b) = (bitmapCardinality(bitmapAnd(a, b)) > 0);
WITH
    bitmapBuild(arrayMap(x -> toInt16(x), range(40))) AS a,
    bitmapBuild(arrayMap(x -> toInt16(x + 20), range(40))) AS b
SELECT
    bitmapAndCardinality(a, b) = bitmapCardinality(bitmapAnd(a, b)),
    bitmapHasAny(a, b) = (bitmapCardinality(bitmapAnd(a, b)) > 0);

SELECT '--- empty operands ---';
WITH
    bitmapBuild(arrayMap(x -> toUInt32(x), range(40))) AS a,
    bitmapBuild([]::Array(UInt32)) AS e
SELECT
    bitmapAndCardinality(a, e) = 0,
    bitmapAndCardinality(e, a) = 0,
    bitmapHasAny(a, e) = 0,
    bitmapHasAny(e, a) = 0,
    bitmapOrCardinality(a, e) = 40,
    bitmapAndnotCardinality(a, e) = 40,
    bitmapAndnotCardinality(e, a) = 0;
