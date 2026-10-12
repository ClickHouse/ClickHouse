-- `uniqCombined` merge where the right-hand state is in a bigger container than the left-hand one
-- (in particular, merge into an empty state): the right-hand container is copied and the left-hand values are inserted into it.
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
SELECT a.fn, a.name, b.name,
    multiIf(a.fn = 'uniqCombined', arrayReduce('uniqCombinedMerge', [a.s, b.s]),
        a.fn = 'uniqCombined64', arrayReduce('uniqCombined64Merge', [a.s64, b.s64]),
        arrayReduce('uniqCombinedMerge(12)', [a.s12, b.s12])) AS ab,
    ab = multiIf(a.fn = 'uniqCombined', arrayReduce('uniqCombinedMerge', [b.s, a.s]),
        a.fn = 'uniqCombined64', arrayReduce('uniqCombined64Merge', [b.s64, a.s64]),
        arrayReduce('uniqCombinedMerge(12)', [b.s12, a.s12])),
    ab = multiIf(a.fn = 'uniqCombined', arrayReduce('uniqCombined', arrayConcat(range(a.lo, a.hi), range(b.lo, b.hi))),
        a.fn = 'uniqCombined64', arrayReduce('uniqCombined64', arrayConcat(range(a.lo, a.hi), range(b.lo, b.hi))),
        arrayReduce('uniqCombined(12)', arrayConcat(range(a.lo, a.hi), range(b.lo, b.hi))))
FROM states AS a, states AS b
WHERE a.fn = b.fn AND (a.name, b.name) IN [
    ('empty', 'medium'), ('empty', 'large'),
    ('small', 'medium'), ('small_disjoint', 'medium'),
    ('small', 'large'), ('small_disjoint', 'large'),
    ('medium', 'large'), ('medium_disjoint', 'large'),
    ('small_full', 'medium_almost_full')]
ORDER BY a.fn, a.name, b.name;

DROP TABLE ranges;
DROP TABLE states;
