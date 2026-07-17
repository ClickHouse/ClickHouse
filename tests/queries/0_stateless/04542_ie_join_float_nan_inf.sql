-- Tags: no-old-analyzer

-- Float keys with NaN, infinities and negative zero, verified against the cross-join oracle
-- (comma join with the conditions in WHERE; `cross_to_inner_join_rewrite = 0` keeps it out of
-- IEJoin). The join predicates follow IEEE semantics: every comparison involving NaN is false,
-- so a NaN-keyed row can never match anything and must come back only as an unmatched row of
-- an outer kind. The operator excludes NaN keys from the sorted union exactly like NULL keys.

SET join_algorithm = 'direct,parallel_hash,hash,ie_join';
SET cross_to_inner_join_rewrite = 0;
SET max_block_size = 16;

DROP TABLE IF EXISTS nan_l;
DROP TABLE IF EXISTS nan_r;

CREATE TABLE nan_l (id Int32, x Float64, y Float32, nx Nullable(Float64)) ENGINE = MergeTree ORDER BY id;
CREATE TABLE nan_r (id Int32, x Float64, y Float32, nx Nullable(Float64)) ENGINE = MergeTree ORDER BY id;

INSERT INTO nan_l SELECT
    number,
    [nan, -nan, inf, -inf, toFloat64('-0'), 0., 1.5, -2.5, 3., 1.5][1 + number % 10],
    toFloat32([nan, inf, -inf, toFloat64('-0'), 0., 2.5, -1.5, 2.5][1 + number % 8]),
    [NULL, nan, 1.5, -2.5, 0., inf][1 + number % 6]
FROM numbers(64);

INSERT INTO nan_r SELECT
    number,
    [nan, -nan, inf, -inf, toFloat64('-0'), 0., 2.5, -1.5, 1.5, -3.][1 + intDiv(number, 2) % 10],
    toFloat32([nan, inf, -inf, toFloat64('-0'), 0., 1.5, -2.5, 3.5][1 + intDiv(number, 3) % 8]),
    [NULL, nan, 2.5, -1.5, 0., -inf][1 + intDiv(number, 5) % 6]
FROM numbers(64);

-- The comparisons below are vacuous if the JOIN side is not routed through IEJoin: pin the plan.
SELECT 'plan', count() > 0 FROM (EXPLAIN SELECT count() FROM nan_l l JOIN nan_r r ON l.x < r.x AND l.y > r.y) WHERE explain LIKE '%IEJoin%';

-- INNER against the oracle, all four direction combinations.
SELECT '<  >', (SELECT (count(), sum(cityHash64(l.id, r.id))) FROM nan_l l JOIN nan_r r ON l.x < r.x AND l.y > r.y) = (SELECT (count(), sum(cityHash64(l.id, r.id))) FROM nan_l l, nan_r r WHERE l.x < r.x AND l.y > r.y) AS ok, (SELECT count() FROM nan_l l JOIN nan_r r ON l.x < r.x AND l.y > r.y) AS cnt;
SELECT '<= >=', (SELECT (count(), sum(cityHash64(l.id, r.id))) FROM nan_l l JOIN nan_r r ON l.x <= r.x AND l.y >= r.y) = (SELECT (count(), sum(cityHash64(l.id, r.id))) FROM nan_l l, nan_r r WHERE l.x <= r.x AND l.y >= r.y) AS ok, (SELECT count() FROM nan_l l JOIN nan_r r ON l.x <= r.x AND l.y >= r.y) AS cnt;
SELECT '>  <=', (SELECT (count(), sum(cityHash64(l.id, r.id))) FROM nan_l l JOIN nan_r r ON l.x > r.x AND l.y <= r.y) = (SELECT (count(), sum(cityHash64(l.id, r.id))) FROM nan_l l, nan_r r WHERE l.x > r.x AND l.y <= r.y) AS ok, (SELECT count() FROM nan_l l JOIN nan_r r ON l.x > r.x AND l.y <= r.y) AS cnt;
SELECT '>= <', (SELECT (count(), sum(cityHash64(l.id, r.id))) FROM nan_l l JOIN nan_r r ON l.x >= r.x AND l.y < r.y) = (SELECT (count(), sum(cityHash64(l.id, r.id))) FROM nan_l l, nan_r r WHERE l.x >= r.x AND l.y < r.y) AS ok, (SELECT count() FROM nan_l l JOIN nan_r r ON l.x >= r.x AND l.y < r.y) AS cnt;

-- No emitted pair may involve a NaN key on either side of either condition.
SELECT 'no nan pairs', count() FROM nan_l l JOIN nan_r r ON l.x <= r.x AND l.y >= r.y WHERE isNaN(l.x) OR isNaN(r.x) OR isNaN(l.y) OR isNaN(r.y);

-- A Nullable Float key mixing NULL and NaN rows: both kinds of rows match nothing.
SELECT 'nullable nan', (SELECT (count(), sum(cityHash64(l.id, r.id))) FROM nan_l l JOIN nan_r r ON l.nx < r.nx AND l.y > r.y) = (SELECT (count(), sum(cityHash64(l.id, r.id))) FROM nan_l l, nan_r r WHERE l.nx < r.nx AND l.y > r.y) AS ok, (SELECT count() FROM nan_l l JOIN nan_r r ON l.nx < r.nx AND l.y > r.y) AS cnt;

-- LEFT: the count oracle is inner pairs plus left rows without a match, so NaN-keyed left rows
-- must all come back exactly once as unmatched.
SELECT 'left', (SELECT count() FROM nan_l l LEFT JOIN nan_r r ON l.x < r.x AND l.y > r.y) = (SELECT (SELECT count() FROM nan_l l, nan_r r WHERE l.x < r.x AND l.y > r.y) + (SELECT count() FROM nan_l) - (SELECT uniqExact(l.id) FROM nan_l l, nan_r r WHERE l.x < r.x AND l.y > r.y)) AS ok, (SELECT count() FROM nan_l l LEFT JOIN nan_r r ON l.x < r.x AND l.y > r.y) AS cnt;

-- SEMI/ANTI: a NaN-keyed left row never matches, so SEMI drops it and ANTI keeps it.
SELECT 'semi', (SELECT arraySort(groupArray(l.id)) FROM nan_l l LEFT SEMI JOIN nan_r r ON l.x < r.x AND l.y > r.y) = (SELECT arraySort(groupArrayDistinct(l.id)) FROM nan_l l, nan_r r WHERE l.x < r.x AND l.y > r.y) AS ok, (SELECT count() FROM nan_l l LEFT SEMI JOIN nan_r r ON l.x < r.x AND l.y > r.y) AS cnt;
SELECT 'anti', (SELECT arraySort(groupArray(l.id)) FROM nan_l l LEFT ANTI JOIN nan_r r ON l.x < r.x AND l.y > r.y) = (SELECT arraySort(groupArray(id)) FROM nan_l WHERE id NOT IN (SELECT l.id FROM nan_l l, nan_r r WHERE l.x < r.x AND l.y > r.y)) AS ok, (SELECT count() FROM nan_l l LEFT ANTI JOIN nan_r r ON l.x < r.x AND l.y > r.y) AS cnt;

DROP TABLE nan_l;
DROP TABLE nan_r;
