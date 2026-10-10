-- Tags: no-replicated-database, no-parallel-replicas, no-random-merge-tree-settings
-- Closes: https://github.com/ClickHouse/ClickHouse/issues/114630

-- Primary key analysis of `pointInPolygon` must not prune with a polygon that
-- `boost::geometry::is_valid` rejects: boost's predicate disagrees with the grid algorithm the
-- function itself evaluates, so granules holding matching rows were dropped.
-- `pip_pk` allows a key-range atom, `pip_nopk` holds the same rows and allows none, so the two
-- counts must always agree. Both `KeyCondition::checkInHyperrectangle` overloads are exercised:
-- `use_lightweight_primary_key_index_analysis` selects between them and `clickhouse-test`
-- randomizes it, so every case pins it explicitly.

-- EXPLAIN output may differ between old and new format
SET explain_query_plan_default = 'legacy';

DROP TABLE IF EXISTS pip_pk;
DROP TABLE IF EXISTS pip_nopk;

CREATE TABLE pip_pk   (x Float64, y Float64) ENGINE = MergeTree ORDER BY (x, y)
    SETTINGS index_granularity = 8, index_granularity_bytes = 0;
CREATE TABLE pip_nopk (x Float64, y Float64) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS index_granularity = 8, index_granularity_bytes = 0;

INSERT INTO pip_pk SELECT (number % 100) * 0.1, (intDiv(number, 100) % 100) * 0.1 FROM numbers(10000);
INSERT INTO pip_nopk SELECT * FROM pip_pk;

-- Under the default `validate_polygons = 1` the function rejects the invalid ring while folding
-- the constant, before any part is selected, so index analysis never sees it.
SELECT count() FROM pip_pk WHERE pointInPolygon((x, y), [(0., 0.), (4., 4.), (4., 0.), (0., 4.)]); -- { serverError BAD_ARGUMENTS }

SET validate_polygons = 0;

-- Self-intersecting ("bowtie") ring, two-argument form.
SELECT 'bowtie 2-arg, dense',
    (SELECT count() FROM pip_pk WHERE pointInPolygon((x, y), [(0., 0.), (4., 4.), (4., 0.), (0., 4.)])
        SETTINGS use_lightweight_primary_key_index_analysis = 0),
    (SELECT count() FROM pip_nopk WHERE pointInPolygon((x, y), [(0., 0.), (4., 4.), (4., 0.), (0., 4.)])),
    (SELECT countIf(pointInPolygon((x, y), [(0., 0.), (4., 4.), (4., 0.), (0., 4.)])) FROM pip_pk);

SELECT 'bowtie 2-arg, lightweight',
    (SELECT count() FROM pip_pk WHERE pointInPolygon((x, y), [(0., 0.), (4., 4.), (4., 0.), (0., 4.)])
        SETTINGS use_lightweight_primary_key_index_analysis = 1),
    (SELECT count() FROM pip_nopk WHERE pointInPolygon((x, y), [(0., 0.), (4., 4.), (4., 0.), (0., 4.)])),
    (SELECT countIf(pointInPolygon((x, y), [(0., 0.), (4., 4.), (4., 0.), (0., 4.)])) FROM pip_pk);

-- Same ring with a hole, three-argument form. It reaches the same analysis lambda (holes are
-- ignored there), so it is affected identically.
SELECT 'bowtie 3-arg, dense',
    (SELECT count() FROM pip_pk WHERE pointInPolygon((x, y), [(0., 0.), (4., 4.), (4., 0.), (0., 4.)], [(1., 1.), (2., 1.), (2., 2.), (1., 2.)])
        SETTINGS use_lightweight_primary_key_index_analysis = 0),
    (SELECT count() FROM pip_nopk WHERE pointInPolygon((x, y), [(0., 0.), (4., 4.), (4., 0.), (0., 4.)], [(1., 1.), (2., 1.), (2., 2.), (1., 2.)]));

SELECT 'bowtie 3-arg, lightweight',
    (SELECT count() FROM pip_pk WHERE pointInPolygon((x, y), [(0., 0.), (4., 4.), (4., 0.), (0., 4.)], [(1., 1.), (2., 1.), (2., 2.), (1., 2.)])
        SETTINGS use_lightweight_primary_key_index_analysis = 1),
    (SELECT count() FROM pip_nopk WHERE pointInPolygon((x, y), [(0., 0.), (4., 4.), (4., 0.), (0., 4.)], [(1., 1.), (2., 1.), (2., 2.), (1., 2.)]));

-- The atom must be DECLINED, not merely widened to a bound that happens to keep every matching
-- row: with no key-range atom left, `force_primary_key` has no primary key use to force.
SELECT count() FROM pip_pk WHERE pointInPolygon((x, y), [(0., 0.), (4., 4.), (4., 0.), (0., 4.)])
    SETTINGS force_primary_key = 1, use_lightweight_primary_key_index_analysis = 0; -- { serverError INDEX_NOT_USED }

SELECT count() FROM pip_pk WHERE pointInPolygon((x, y), [(0., 0.), (4., 4.), (4., 0.), (0., 4.)])
    SETTINGS force_primary_key = 1, use_lightweight_primary_key_index_analysis = 1; -- { serverError INDEX_NOT_USED }

SELECT count() FROM pip_pk WHERE pointInPolygon((x, y), [(0., 0.), (4., 4.), (4., 0.), (0., 4.)], [(1., 1.), (2., 1.), (2., 2.), (1., 2.)])
    SETTINGS force_primary_key = 1, use_lightweight_primary_key_index_analysis = 0; -- { serverError INDEX_NOT_USED }

SELECT count() FROM pip_pk WHERE pointInPolygon((x, y), [(0., 0.), (4., 4.), (4., 0.), (0., 4.)], [(1., 1.), (2., 1.), (2., 2.), (1., 2.)])
    SETTINGS force_primary_key = 1, use_lightweight_primary_key_index_analysis = 1; -- { serverError INDEX_NOT_USED }

-- No row is lost, and none is gained either: pruning must be an exact filter here.
SELECT 'lost rows', count() FROM
(
    SELECT x, y FROM pip_nopk WHERE pointInPolygon((x, y), [(0., 0.), (4., 4.), (4., 0.), (0., 4.)])
    EXCEPT
    SELECT x, y FROM pip_pk WHERE pointInPolygon((x, y), [(0., 0.), (4., 4.), (4., 0.), (0., 4.)])
);

SELECT 'extra rows', count() FROM
(
    SELECT x, y FROM pip_pk WHERE pointInPolygon((x, y), [(0., 0.), (4., 4.), (4., 0.), (0., 4.)])
    EXCEPT
    SELECT x, y FROM pip_nopk WHERE pointInPolygon((x, y), [(0., 0.), (4., 4.), (4., 0.), (0., 4.)])
);

-- Hole rings are not stored, so pruning tests the shell alone. That is only an over-approximation
-- of the function while the assembled shell-plus-holes shape is valid. Here the shell is valid and
-- concave and the hole self-intersects: the function reports points inside the shell's notch, which
-- the shell alone excludes, so the two counts diverged by 110 rows before the assembled shape was
-- validated. A convex shell cannot expose this, having no notch outside its own outline.
SELECT 'invalid hole, dense',
    (SELECT count() FROM pip_pk WHERE pointInPolygon((x, y), [(0., 0.), (8., 0.), (8., 4.), (4., 4.), (4., 8.), (0., 8.)], [(1., 1.), (7., 7.), (7., 1.), (1., 7.)])
        SETTINGS use_lightweight_primary_key_index_analysis = 0),
    (SELECT count() FROM pip_nopk WHERE pointInPolygon((x, y), [(0., 0.), (8., 0.), (8., 4.), (4., 4.), (4., 8.), (0., 8.)], [(1., 1.), (7., 7.), (7., 1.), (1., 7.)]));

SELECT 'invalid hole, lightweight',
    (SELECT count() FROM pip_pk WHERE pointInPolygon((x, y), [(0., 0.), (8., 0.), (8., 4.), (4., 4.), (4., 8.), (0., 8.)], [(1., 1.), (7., 7.), (7., 1.), (1., 7.)])
        SETTINGS use_lightweight_primary_key_index_analysis = 1),
    (SELECT count() FROM pip_nopk WHERE pointInPolygon((x, y), [(0., 0.), (8., 0.), (8., 4.), (4., 4.), (4., 8.), (0., 8.)], [(1., 1.), (7., 7.), (7., 1.), (1., 7.)]));

SELECT 'invalid hole, lost rows', count() FROM
(
    SELECT x, y FROM pip_nopk WHERE pointInPolygon((x, y), [(0., 0.), (8., 0.), (8., 4.), (4., 4.), (4., 8.), (0., 8.)], [(1., 1.), (7., 7.), (7., 1.), (1., 7.)])
    EXCEPT
    SELECT x, y FROM pip_pk WHERE pointInPolygon((x, y), [(0., 0.), (8., 0.), (8., 4.), (4., 4.), (4., 8.), (0., 8.)], [(1., 1.), (7., 7.), (7., 1.), (1., 7.)])
);

SELECT count() FROM pip_pk WHERE pointInPolygon((x, y), [(0., 0.), (8., 0.), (8., 4.), (4., 4.), (4., 8.), (0., 8.)], [(1., 1.), (7., 7.), (7., 1.), (1., 7.)])
    SETTINGS force_primary_key = 1, use_lightweight_primary_key_index_analysis = 0; -- { serverError INDEX_NOT_USED }

SELECT count() FROM pip_pk WHERE pointInPolygon((x, y), [(0., 0.), (8., 0.), (8., 4.), (4., 4.), (4., 8.), (0., 8.)], [(1., 1.), (7., 7.), (7., 1.), (1., 7.)])
    SETTINGS force_primary_key = 1, use_lightweight_primary_key_index_analysis = 1; -- { serverError INDEX_NOT_USED }

-- Every hole argument takes part, and validity is a property of the assembly rather than of the
-- rings: here the shell and both holes are individually valid, yet the second hole lies outside the
-- shell, so the assembled polygon is not. Validating the rings one by one, or only the first hole,
-- would build an atom here.
SELECT count() FROM pip_pk WHERE pointInPolygon((x, y), [(0., 0.), (8., 0.), (8., 4.), (4., 4.), (4., 8.), (0., 8.)], [(1., 1.), (3., 1.), (3., 3.), (1., 3.)], [(20., 20.), (21., 20.), (21., 21.), (20., 21.)])
    SETTINGS force_primary_key = 1, use_lightweight_primary_key_index_analysis = 0; -- { serverError INDEX_NOT_USED }

SELECT count() FROM pip_pk WHERE pointInPolygon((x, y), [(0., 0.), (8., 0.), (8., 4.), (4., 4.), (4., 8.), (0., 8.)], [(1., 1.), (3., 1.), (3., 3.), (1., 3.)], [(20., 20.), (21., 20.), (21., 21.), (20., 21.)])
    SETTINGS force_primary_key = 1, use_lightweight_primary_key_index_analysis = 1; -- { serverError INDEX_NOT_USED }

SELECT 'hole outside shell, counts',
    (SELECT count() FROM pip_pk WHERE pointInPolygon((x, y), [(0., 0.), (8., 0.), (8., 4.), (4., 4.), (4., 8.), (0., 8.)], [(1., 1.), (3., 1.), (3., 3.), (1., 3.)], [(20., 20.), (21., 20.), (21., 21.), (20., 21.)])
        SETTINGS use_lightweight_primary_key_index_analysis = 0),
    (SELECT count() FROM pip_nopk WHERE pointInPolygon((x, y), [(0., 0.), (8., 0.), (8., 4.), (4., 4.), (4., 8.), (0., 8.)], [(1., 1.), (3., 1.), (3., 3.), (1., 3.)], [(20., 20.), (21., 20.), (21., 21.), (20., 21.)]));

-- The same shell with a hole that keeps the assembled shape valid must keep both its result and its
-- pruning, so that the check rejects invalid assemblies rather than every hole argument.
SELECT 'valid hole, dense',
    (SELECT count() FROM pip_pk WHERE pointInPolygon((x, y), [(0., 0.), (8., 0.), (8., 4.), (4., 4.), (4., 8.), (0., 8.)], [(1., 1.), (3., 1.), (3., 3.), (1., 3.)])
        SETTINGS use_lightweight_primary_key_index_analysis = 0),
    (SELECT count() FROM pip_nopk WHERE pointInPolygon((x, y), [(0., 0.), (8., 0.), (8., 4.), (4., 4.), (4., 8.), (0., 8.)], [(1., 1.), (3., 1.), (3., 3.), (1., 3.)]));

SELECT 'valid hole forces key, dense', count() FROM pip_pk
    WHERE pointInPolygon((x, y), [(0., 0.), (8., 0.), (8., 4.), (4., 4.), (4., 8.), (0., 8.)], [(1., 1.), (3., 1.), (3., 3.), (1., 3.)])
    SETTINGS force_primary_key = 1, use_lightweight_primary_key_index_analysis = 0,
        use_query_condition_cache = 0;

SELECT 'valid hole forces key, lightweight', count() FROM pip_pk
    WHERE pointInPolygon((x, y), [(0., 0.), (8., 0.), (8., 4.), (4., 4.), (4., 8.), (0., 8.)], [(1., 1.), (3., 1.), (3., 3.), (1., 3.)])
    SETTINGS force_primary_key = 1, use_lightweight_primary_key_index_analysis = 1,
        use_query_condition_cache = 0;

-- A ring with too few points is also rejected by `is_valid`, so it must stay consistent too.
SELECT 'degenerate ring, dense',
    (SELECT count() FROM pip_pk WHERE pointInPolygon((x, y), [(0., 0.), (1., 1.)])
        SETTINGS use_lightweight_primary_key_index_analysis = 0),
    (SELECT count() FROM pip_nopk WHERE pointInPolygon((x, y), [(0., 0.), (1., 1.)]));

SELECT 'degenerate ring, lightweight',
    (SELECT count() FROM pip_pk WHERE pointInPolygon((x, y), [(0., 0.), (1., 1.)])
        SETTINGS use_lightweight_primary_key_index_analysis = 1),
    (SELECT count() FROM pip_nopk WHERE pointInPolygon((x, y), [(0., 0.), (1., 1.)]));

SELECT count() FROM pip_pk WHERE pointInPolygon((x, y), [(0., 0.), (1., 1.)])
    SETTINGS force_primary_key = 1, use_lightweight_primary_key_index_analysis = 0; -- { serverError INDEX_NOT_USED }

SELECT count() FROM pip_pk WHERE pointInPolygon((x, y), [(0., 0.), (1., 1.)])
    SETTINGS force_primary_key = 1, use_lightweight_primary_key_index_analysis = 1; -- { serverError INDEX_NOT_USED }

-- A valid polygon must keep both its result and its pruning: the check must not reject shapes
-- that were being pruned correctly.
SELECT 'valid ring, dense',
    (SELECT count() FROM pip_pk WHERE pointInPolygon((x, y), [(1., 1.), (1., 5.), (5., 5.), (5., 1.)])
        SETTINGS use_lightweight_primary_key_index_analysis = 0),
    (SELECT count() FROM pip_nopk WHERE pointInPolygon((x, y), [(1., 1.), (1., 5.), (5., 5.), (5., 1.)]));

SELECT 'valid ring, lightweight',
    (SELECT count() FROM pip_pk WHERE pointInPolygon((x, y), [(1., 1.), (1., 5.), (5., 5.), (5., 1.)])
        SETTINGS use_lightweight_primary_key_index_analysis = 1),
    (SELECT count() FROM pip_nopk WHERE pointInPolygon((x, y), [(1., 1.), (1., 5.), (5., 5.), (5., 1.)]));

-- A valid ring still builds an atom, so forcing the primary key must not raise.
SELECT 'valid ring forces key, dense', count() FROM pip_pk
    WHERE pointInPolygon((x, y), [(1., 1.), (1., 5.), (5., 5.), (5., 1.)])
    SETTINGS force_primary_key = 1, use_lightweight_primary_key_index_analysis = 0,
        use_query_condition_cache = 0;

SELECT 'valid ring forces key, lightweight', count() FROM pip_pk
    WHERE pointInPolygon((x, y), [(1., 1.), (1., 5.), (5., 5.), (5., 1.)])
    SETTINGS force_primary_key = 1, use_lightweight_primary_key_index_analysis = 1,
        use_query_condition_cache = 0;

-- Pruning must stay effective, not merely non-zero: this ring selects about 23% of the granules,
-- the bound admits anything under 25%, and a ring that prunes nothing reads 100%. The seek
-- thresholds inflate that count by merging ranges across gaps, the condition cache lowers it.
SELECT 'valid ring prunes, dense',
    toUInt64(extract(explain, 'Granules: ([0-9]+)')) * 4 < toUInt64(extract(explain, 'Granules: [0-9]+/([0-9]+)'))
FROM
(
    EXPLAIN indexes = 1
    SELECT count() FROM pip_pk WHERE pointInPolygon((x, y), [(1., 1.), (1., 5.), (5., 5.), (5., 1.)])
    SETTINGS use_lightweight_primary_key_index_analysis = 0,
        merge_tree_min_rows_for_seek = 0, merge_tree_min_bytes_for_seek = 0,
        use_query_condition_cache = 0
)
WHERE explain LIKE '%Granules%';

SELECT 'valid ring prunes, lightweight',
    toUInt64(extract(explain, 'Granules: ([0-9]+)')) * 4 < toUInt64(extract(explain, 'Granules: [0-9]+/([0-9]+)'))
FROM
(
    EXPLAIN indexes = 1
    SELECT count() FROM pip_pk WHERE pointInPolygon((x, y), [(1., 1.), (1., 5.), (5., 5.), (5., 1.)])
    SETTINGS use_lightweight_primary_key_index_analysis = 1,
        merge_tree_min_rows_for_seek = 0, merge_tree_min_bytes_for_seek = 0,
        use_query_condition_cache = 0
)
WHERE explain LIKE '%Granules%';

DROP TABLE pip_pk;
DROP TABLE pip_nopk;
