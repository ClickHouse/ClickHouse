-- `uniqCombined` merge where the right-hand state is in a bigger container than the left-hand one
-- (in particular, merge into an empty state): the right-hand container is copied and the left-hand values are inserted into it.
-- With the default K = 17 and 32-bit hashes, the small set holds 16 values and the medium hash set holds 8192 values.

DROP TABLE IF EXISTS ranges;
DROP TABLE IF EXISTS states;

CREATE TABLE ranges (name String, lo UInt64, hi UInt64) ENGINE = Memory;
INSERT INTO ranges VALUES
    ('empty', 0, 0),
    ('small', 0, 10), ('small_disjoint', 200000, 200010), ('small_full', 400000, 400016),
    ('medium', 0, 1000), ('medium_disjoint', 300000, 301000), ('medium_almost_full', 500000, 508190),
    ('large', 0, 100000);

CREATE TABLE states (name String, lo UInt64, hi UInt64, s AggregateFunction(uniqCombined, UInt64)) ENGINE = Memory;
INSERT INTO states SELECT name, lo, hi, arrayReduce('uniqCombinedState', range(lo, hi)) FROM ranges;

SELECT 'merge into an empty state gives the same state';
SELECT name, finalizeAggregation(s), hex(arrayReduce('uniqCombinedMergeState', [s])) = hex(s)
FROM states WHERE name IN ('small', 'medium', 'large') ORDER BY hi - lo;

SELECT 'merge with a state in a bigger container, both orders, compared with direct aggregation';
SELECT a.name, b.name,
    arrayReduce('uniqCombinedMerge', [a.s, b.s]) AS ab,
    ab = arrayReduce('uniqCombinedMerge', [b.s, a.s]),
    ab = arrayReduce('uniqCombined', arrayConcat(range(a.lo, a.hi), range(b.lo, b.hi)))
FROM states AS a, states AS b
WHERE (a.name, b.name) IN [
    ('empty', 'medium'), ('empty', 'large'),
    ('small', 'medium'), ('small_disjoint', 'medium'),
    ('small', 'large'), ('small_disjoint', 'large'),
    ('medium', 'large'), ('medium_disjoint', 'large'),
    ('small_full', 'medium_almost_full')]
ORDER BY a.name, b.name;

DROP TABLE ranges;
DROP TABLE states;
