-- `uniqCombined` merge where the right-hand state is in a bigger container than the left-hand one (in particular, merge into an empty state).
-- A right-hand `HyperLogLogCounter` is copied and the left-hand values are inserted into it. The results must not change,
-- including those which depend on the order of insertion into the medium set (the last two queries).
-- The small set holds 16 values. The medium hash set holds 2^(K - 5) values with 64-bit hashes and twice as many with 32-bit hashes:
-- 8192 for `uniqCombined` (K = 17, 32-bit), 4096 for `uniqCombined64` (K = 17, 64-bit), 256 for `uniqCombined(12)` (32-bit).

DROP TABLE IF EXISTS ranges;
DROP TABLE IF EXISTS states;

CREATE TABLE ranges (fn String, name String, lo UInt64, hi UInt64) ENGINE = Memory;
INSERT INTO ranges SELECT fn, r.1, r.2, r.3
FROM (SELECT arrayJoin([('uniqCombined', 8192), ('uniqCombined64', 4096), ('uniqCombined(12)', 256)]::Array(Tuple(String, UInt64))) AS t, t.1 AS fn, t.2 AS medium_max)
ARRAY JOIN [
    ('empty', 0, 0),
    ('small', 0, 10), ('small_disjoint', 200000, 200010), ('small_full', 400000, 400016),
    ('medium', 0, medium_max / 8), ('medium_disjoint', 300000, 300000 + medium_max / 8), ('medium_almost_full', 500000, 500000 + medium_max - 2),
    ('large', 0, 100000)]::Array(Tuple(String, UInt64, UInt64)) AS r;

CREATE TABLE states
(
    fn String, name String, lo UInt64, hi UInt64,
    s AggregateFunction(uniqCombined, UInt64),
    s64 AggregateFunction(uniqCombined64, UInt64),
    s12 AggregateFunction(uniqCombined(12), UInt64)
) ENGINE = Memory;
INSERT INTO states SELECT fn, name, lo, hi,
    arrayReduce('uniqCombinedState', range(lo, hi)),
    arrayReduce('uniqCombined64State', range(lo, hi)),
    arrayReduce('uniqCombinedState(12)', range(lo, hi))
FROM ranges;

SELECT 'merge into an empty state gives the same state';
SELECT fn, name,
    multiIf(fn = 'uniqCombined', finalizeAggregation(s), fn = 'uniqCombined64', finalizeAggregation(s64), finalizeAggregation(s12)),
    multiIf(fn = 'uniqCombined', hex(arrayReduce('uniqCombinedMergeState', [s])) = hex(s),
        fn = 'uniqCombined64', hex(arrayReduce('uniqCombined64MergeState', [s64])) = hex(s64),
        hex(arrayReduce('uniqCombinedMergeState(12)', [s12])) = hex(s12))
FROM states WHERE name IN ('small', 'medium', 'large') ORDER BY fn, hi - lo;

SELECT 'merge with a state in a bigger container, both orders, compared with direct aggregation';
WITH arrayConcat(range(a.lo, a.hi), range(b.lo, b.hi)) AS vals
SELECT a.fn, a.name, b.name,
    multiIf(a.fn = 'uniqCombined', arrayReduce('uniqCombinedMerge', [a.s, b.s]),
        a.fn = 'uniqCombined64', arrayReduce('uniqCombined64Merge', [a.s64, b.s64]),
        arrayReduce('uniqCombinedMerge(12)', [a.s12, b.s12])) AS ab,
    ab = multiIf(a.fn = 'uniqCombined', arrayReduce('uniqCombinedMerge', [b.s, a.s]),
        a.fn = 'uniqCombined64', arrayReduce('uniqCombined64Merge', [b.s64, a.s64]),
        arrayReduce('uniqCombinedMerge(12)', [b.s12, a.s12])),
    ab = multiIf(a.fn = 'uniqCombined', arrayReduce('uniqCombined', vals),
        a.fn = 'uniqCombined64', arrayReduce('uniqCombined64', vals),
        arrayReduce('uniqCombined(12)', vals))
FROM states AS a, states AS b
WHERE a.fn = b.fn AND (a.name, b.name) IN [
    ('empty', 'medium'), ('empty', 'large'),
    ('small', 'medium'), ('small_disjoint', 'medium'),
    ('small', 'large'), ('small_disjoint', 'large'),
    ('medium', 'large'), ('medium_disjoint', 'large'),
    ('small_full', 'medium_almost_full')]
ORDER BY a.fn, a.name, b.name;

SELECT 'merge of a small state into a medium state which the union fills, both orders';
-- Either a full medium set and a small set of its values only, or a medium set with one free slot and a small set of a new value followed by one of its values.
SELECT fn, case,
    multiIf(fn = 'uniqCombined', arrayReduce('uniqCombinedMerge', [arrayReduce('uniqCombinedState', lhs), arrayReduce('uniqCombinedState', rhs)]),
        fn = 'uniqCombined64', arrayReduce('uniqCombined64Merge', [arrayReduce('uniqCombined64State', lhs), arrayReduce('uniqCombined64State', rhs)]),
        arrayReduce('uniqCombinedMerge(12)', [arrayReduce('uniqCombinedState(12)', lhs), arrayReduce('uniqCombinedState(12)', rhs)])),
    multiIf(fn = 'uniqCombined', arrayReduce('uniqCombinedMerge', [arrayReduce('uniqCombinedState', rhs), arrayReduce('uniqCombinedState', lhs)]),
        fn = 'uniqCombined64', arrayReduce('uniqCombined64Merge', [arrayReduce('uniqCombined64State', rhs), arrayReduce('uniqCombined64State', lhs)]),
        arrayReduce('uniqCombinedMerge(12)', [arrayReduce('uniqCombinedState(12)', rhs), arrayReduce('uniqCombinedState(12)', lhs)]))
FROM
(
    SELECT t.1 AS fn, t.2 AS medium_max, c.1 AS case, c.2 AS lhs, c.3 AS rhs
    FROM (SELECT arrayJoin([('uniqCombined', 8192), ('uniqCombined64', 4096), ('uniqCombined(12)', 256)]::Array(Tuple(String, UInt64))) AS t)
    ARRAY JOIN [
        ('only_duplicates', arrayMap(i -> i * intDiv(medium_max, 16), range(16)), range(medium_max)),
        ('new_then_duplicate', [1000000, 0], range(medium_max - 1))]::Array(Tuple(String, Array(UInt64), Array(UInt64))) AS c
)
ORDER BY fn, case;

SELECT 'merge of a medium state, which is the result of a merge of a small state into a medium state, at the boundary of the medium set';
WITH
    arrayReduce('uniqCombinedState(12)', [toUInt64(1000443)]) AS a,
    arrayReduce('uniqCombinedState(12)', range(toUInt64(255))) AS b,
    arrayReduce('uniqCombinedMergeState(12)', [a, b]) AS s,
    arrayReduce('uniqCombinedState(12)', arrayConcat(range(toUInt64(17)), [toUInt64(1000443)])) AS t
SELECT arrayReduce('uniqCombinedMerge(12)', [t, s]);

DROP TABLE ranges;
DROP TABLE states;
