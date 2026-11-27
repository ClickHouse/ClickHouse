-- Tags: no-old-analyzer

-- Sparse-serialized key columns (mostly-default values stored sparsely by MergeTree) must be
-- densified on input (`removeConstAndSparse`); verified against the cross-join oracle.
-- `extremes = 1` exercises the extremes drop in the paired join pipeline.

SET join_algorithm = 'ie_join,hash';

DROP TABLE IF EXISTS sp_l;
DROP TABLE IF EXISTS sp_r;

CREATE TABLE sp_l (id UInt32, x Int32, y Int32) ENGINE = MergeTree ORDER BY id
    SETTINGS ratio_of_defaults_for_sparse_serialization = 0.5;
CREATE TABLE sp_r (id UInt32, x Int32, y Int32) ENGINE = MergeTree ORDER BY id
    SETTINGS ratio_of_defaults_for_sparse_serialization = 0.5;

INSERT INTO sp_l SELECT number + 1, if(number % 10 = 0, toInt32(number % 37), 0), if(number % 8 = 0, toInt32(number % 29), 0) FROM numbers(400);
INSERT INTO sp_r SELECT number + 1, if(number % 9 = 0, toInt32(number % 31 + 1), 0), if(number % 7 = 0, toInt32(number % 23), 0) FROM numbers(400);

-- The key columns are actually stored sparsely
SELECT 'sparse used', countIf(serialization_kind = 'Sparse') > 0 FROM system.parts_columns
WHERE database = currentDatabase() AND table IN ('sp_l', 'sp_r') AND column IN ('x', 'y') AND active;

SELECT 'routed', count() > 0 FROM (EXPLAIN SELECT count() FROM sp_l l JOIN sp_r r ON l.x < r.x AND l.y > r.y) WHERE explain LIKE '%IEJoin%';

SELECT 'inner', (
    SELECT (count(), sum(cityHash64(l.id, r.id))) FROM sp_l l JOIN sp_r r ON l.x < r.x AND l.y > r.y
) = (
    SELECT (count(), sum(cityHash64(l.id, r.id))) FROM sp_l l, sp_r r WHERE l.x < r.x AND l.y > r.y
) AS ok, (SELECT count() FROM sp_l l JOIN sp_r r ON l.x < r.x AND l.y > r.y) AS cnt;

SELECT 'left', (
    SELECT count() FROM sp_l l LEFT JOIN sp_r r ON l.x < r.x AND l.y > r.y
) = (
    SELECT (SELECT count() FROM sp_l l, sp_r r WHERE l.x < r.x AND l.y > r.y)
         + (SELECT count() FROM sp_l)
         - (SELECT uniqExact(l.id) FROM sp_l l, sp_r r WHERE l.x < r.x AND l.y > r.y)
) AS ok, (SELECT count() FROM sp_l l LEFT JOIN sp_r r ON l.x < r.x AND l.y > r.y) AS cnt;

-- Extremes over the joined result
SELECT l.id + r.id AS pair_sum FROM sp_l l JOIN sp_r r ON l.x < r.x AND l.y > r.y ORDER BY pair_sum LIMIT 3 SETTINGS extremes = 1;

DROP TABLE sp_l;
DROP TABLE sp_r;
