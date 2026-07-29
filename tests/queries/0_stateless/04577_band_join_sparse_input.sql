-- Tags: no-old-analyzer

-- Sparse-serialized MergeTree columns must be densified on input on both sides of the band
-- join: the point side streams into the probe with no sort to launder it, and the interval
-- side reaches the build unlaundered when read-in-order relaxes the pre-sort to a streaming
-- merge over the in-order read. Also covers a sparse column inside a lazily-replicated
-- wrapper (ARRAY JOIN), which must be expanded and densified the same way.

SET join_algorithm = 'band_join,hash';
SET optimize_read_in_order = 1;
-- so the cross-join oracles cannot be routed back into the band join
SET cross_to_inner_join_rewrite = 0;

DROP TABLE IF EXISTS sp_p;
DROP TABLE IF EXISTS sp_i;

-- Every column contains defaults, so with ratio 0 they are all stored sparsely.
CREATE TABLE sp_p (t Int64, y Int64) ENGINE = MergeTree ORDER BY t
    SETTINGS ratio_of_defaults_for_sparse_serialization = 0;
CREATE TABLE sp_i (lo Int64, hi Int64) ENGINE = MergeTree ORDER BY lo
    SETTINGS ratio_of_defaults_for_sparse_serialization = 0;

SYSTEM STOP MERGES sp_i;
INSERT INTO sp_p SELECT if(number % 5 = 0, 0, number % 700), if(number % 3 = 0, 0, number % 41) FROM numbers(2000);
INSERT INTO sp_i SELECT if(number % 4 = 0, 0, number % 600), if(number % 4 = 0, 0, number % 600 + 15) FROM numbers(800);
INSERT INTO sp_i SELECT if(number % 6 = 0, 0, number % 500 + 3), if(number % 6 = 0, 0, number % 500 + 40) FROM numbers(700);

SELECT 'sparse used', countIf(serialization_kind = 'Sparse') = count() FROM system.parts_columns
WHERE database = currentDatabase() AND table IN ('sp_p', 'sp_i') AND active;

SELECT 'routed', count() > 0 FROM (
    EXPLAIN SELECT count() FROM sp_p p JOIN sp_i i ON p.t >= i.lo AND p.t <= i.hi
) WHERE explain LIKE '%BandJoin%';

-- The interval side is read in order, so the sparse columns reach the build transform
-- without a full sort re-materializing them.
SELECT 'in-order read', count() > 0 FROM (
    EXPLAIN actions = 1 SELECT count() FROM sp_p p JOIN sp_i i ON p.t >= i.lo AND p.t <= i.hi
) WHERE explain LIKE '%Read type: InOrder%';

SELECT 'inner', (
    SELECT (count(), sum(cityHash64(p.t, p.y, i.lo, i.hi))) FROM sp_p p JOIN sp_i i ON p.t >= i.lo AND p.t <= i.hi
) = (
    SELECT (count(), sum(cityHash64(p.t, p.y, i.lo, i.hi))) FROM sp_p p, sp_i i WHERE p.t >= i.lo AND p.t <= i.hi
) AS ok, (SELECT count() FROM sp_p p JOIN sp_i i ON p.t >= i.lo AND p.t <= i.hi) AS cnt;

SELECT 'left', (
    SELECT (count(), sum(cityHash64(p.t, p.y, i.lo, i.hi))) FROM sp_p p LEFT JOIN sp_i i ON p.t >= i.lo AND p.t <= i.hi
) = (
    SELECT (count(), sum(cityHash64(p.t, p.y, i.lo, i.hi))) FROM sp_p p LEFT JOIN sp_i i ON p.t >= i.lo AND p.t <= i.hi
    SETTINGS join_algorithm = 'ie_join'
) AS ok, (SELECT count() FROM sp_p p LEFT JOIN sp_i i ON p.t >= i.lo AND p.t <= i.hi) AS cnt;

-- A sparse column inside a lazily-replicated wrapper: ARRAY JOIN over a sparse-serialized
-- table wraps the columns instead of copying them, and no upstream transform expands the
-- wrapper before the join.
SET enable_lazy_columns_replication = 1;

DROP TABLE IF EXISTS sp_lazy;
DROP TABLE IF EXISTS sp_rng;

CREATE TABLE sp_lazy (k UInt64, v UInt64, arr Array(UInt8)) ENGINE = MergeTree ORDER BY k
    SETTINGS ratio_of_defaults_for_sparse_serialization = 0.5;
INSERT INTO sp_lazy SELECT number, if(number % 20000 = 1, number, 0), [1] FROM numbers(200000);

CREATE TABLE sp_rng (lo UInt64, hi UInt64) ENGINE = MergeTree ORDER BY lo;
INSERT INTO sp_rng SELECT number * 20000, number * 20000 + 10 FROM numbers(10);

SELECT 'lazy sparse used', countIf(serialization_kind = 'Sparse') > 0 FROM system.parts_columns
WHERE database = currentDatabase() AND table = 'sp_lazy' AND column = 'v' AND active;

-- Sparse column as the point-side payload
SELECT 'lazy point payload', count(), sum(v) FROM (
    SELECT p.v AS v FROM (SELECT k, v FROM sp_lazy ARRAY JOIN arr) p JOIN sp_rng i ON p.k >= i.lo AND p.k <= i.hi
);
SELECT 'lazy point payload oracle', count(), sum(v) FROM sp_lazy
WHERE arrayExists(n -> (k >= n * 20000 AND k <= n * 20000 + 10), range(10));

-- Sparse column as the point-side key
SELECT 'lazy point key', count(), sum(v) FROM (
    SELECT p.v AS v FROM (SELECT k, v FROM sp_lazy ARRAY JOIN arr) p JOIN sp_rng i ON p.v >= i.lo AND p.v <= i.hi
);
SELECT 'lazy point key oracle', count(), sum(v) FROM sp_lazy
WHERE arrayExists(n -> (v >= n * 20000 AND v <= n * 20000 + 10), range(10));

-- Sparse column as an interval-side payload (the point side is on the left, so the ARRAY
-- JOIN side is built into the index)
SELECT 'lazy interval payload', count(), sum(v) FROM (
    SELECT i.v AS v FROM sp_rng r JOIN (SELECT k, v FROM sp_lazy ARRAY JOIN arr) i ON r.lo >= i.k AND r.lo <= i.k + 5
);
SELECT 'lazy interval payload oracle', count(), sum(v) FROM (
    SELECT i.v AS v FROM sp_rng r, (SELECT k, v FROM sp_lazy ARRAY JOIN arr) i WHERE r.lo >= i.k AND r.lo <= i.k + 5
);

DROP TABLE sp_p;
DROP TABLE sp_i;
DROP TABLE sp_lazy;
DROP TABLE sp_rng;
