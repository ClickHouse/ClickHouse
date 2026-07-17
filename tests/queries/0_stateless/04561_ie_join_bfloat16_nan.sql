-- Tags: no-old-analyzer

-- BFloat16 keys with NaN, verified against the cross-join oracle (comma join with the
-- conditions in WHERE; `cross_to_inner_join_rewrite = 0` keeps it out of IEJoin). BFloat16
-- is not covered by the fixed-width key encoding, so this exercises the generic comparator
-- path; NaN-keyed rows must be excluded from the sorted union exactly like NULL keys.

SET join_algorithm = 'direct,parallel_hash,hash,ie_join';
SET cross_to_inner_join_rewrite = 0;
SET max_block_size = 16;

DROP TABLE IF EXISTS bf_l;
DROP TABLE IF EXISTS bf_r;

CREATE TABLE bf_l (id Int32, x BFloat16, y BFloat16, nx Nullable(BFloat16)) ENGINE = MergeTree ORDER BY id;
CREATE TABLE bf_r (id Int32, x BFloat16, y BFloat16, nx Nullable(BFloat16)) ENGINE = MergeTree ORDER BY id;

INSERT INTO bf_l SELECT
    number,
    [nan, -nan, inf, -inf, 0., 1.5, -2.5, 3.][1 + number % 8]::BFloat16,
    [nan, inf, -inf, 0., 2.5, -1.5][1 + number % 6]::BFloat16,
    [NULL, nan, 1.5, -2.5, 0., inf][1 + number % 6]::Nullable(BFloat16)
FROM numbers(48);

INSERT INTO bf_r SELECT
    number,
    [nan, -nan, inf, -inf, 0., 2.5, -1.5, -3.][1 + intDiv(number, 2) % 8]::BFloat16,
    [nan, inf, -inf, 0., 1.5, -2.5][1 + intDiv(number, 3) % 6]::BFloat16,
    [NULL, nan, 2.5, -1.5, 0., -inf][1 + intDiv(number, 5) % 6]::Nullable(BFloat16)
FROM numbers(48);

-- The comparisons below are vacuous if the JOIN side is not routed through IEJoin: pin the plan.
SELECT 'plan', count() > 0 FROM (EXPLAIN SELECT count() FROM bf_l l JOIN bf_r r ON l.x < r.x AND l.y > r.y) WHERE explain LIKE '%IEJoin%';

-- INNER against the oracle, all four direction combinations.
SELECT '<  >', (SELECT (count(), sum(cityHash64(l.id, r.id))) FROM bf_l l JOIN bf_r r ON l.x < r.x AND l.y > r.y) = (SELECT (count(), sum(cityHash64(l.id, r.id))) FROM bf_l l, bf_r r WHERE l.x < r.x AND l.y > r.y) AS ok, (SELECT count() FROM bf_l l JOIN bf_r r ON l.x < r.x AND l.y > r.y) AS cnt;
SELECT '<= >=', (SELECT (count(), sum(cityHash64(l.id, r.id))) FROM bf_l l JOIN bf_r r ON l.x <= r.x AND l.y >= r.y) = (SELECT (count(), sum(cityHash64(l.id, r.id))) FROM bf_l l, bf_r r WHERE l.x <= r.x AND l.y >= r.y) AS ok, (SELECT count() FROM bf_l l JOIN bf_r r ON l.x <= r.x AND l.y >= r.y) AS cnt;
SELECT '>  <=', (SELECT (count(), sum(cityHash64(l.id, r.id))) FROM bf_l l JOIN bf_r r ON l.x > r.x AND l.y <= r.y) = (SELECT (count(), sum(cityHash64(l.id, r.id))) FROM bf_l l, bf_r r WHERE l.x > r.x AND l.y <= r.y) AS ok, (SELECT count() FROM bf_l l JOIN bf_r r ON l.x > r.x AND l.y <= r.y) AS cnt;
SELECT '>= <', (SELECT (count(), sum(cityHash64(l.id, r.id))) FROM bf_l l JOIN bf_r r ON l.x >= r.x AND l.y < r.y) = (SELECT (count(), sum(cityHash64(l.id, r.id))) FROM bf_l l, bf_r r WHERE l.x >= r.x AND l.y < r.y) AS ok, (SELECT count() FROM bf_l l JOIN bf_r r ON l.x >= r.x AND l.y < r.y) AS cnt;

-- No emitted pair may involve a NaN key on either side of either condition.
SELECT 'no nan pairs', count() FROM bf_l l JOIN bf_r r ON l.x <= r.x AND l.y >= r.y WHERE isNaN(l.x) OR isNaN(r.x) OR isNaN(l.y) OR isNaN(r.y);

-- A Nullable BFloat16 key mixing NULL and NaN rows: both kinds of rows match nothing.
SELECT 'nullable nan', (SELECT (count(), sum(cityHash64(l.id, r.id))) FROM bf_l l JOIN bf_r r ON l.nx < r.nx AND l.y > r.y) = (SELECT (count(), sum(cityHash64(l.id, r.id))) FROM bf_l l, bf_r r WHERE l.nx < r.nx AND l.y > r.y) AS ok, (SELECT count() FROM bf_l l JOIN bf_r r ON l.nx < r.nx AND l.y > r.y) AS cnt;

-- LEFT: the count oracle is inner pairs plus left rows without a match, so NaN-keyed left rows
-- must all come back exactly once as unmatched.
SELECT 'left', (SELECT count() FROM bf_l l LEFT JOIN bf_r r ON l.x < r.x AND l.y > r.y) = (SELECT (SELECT count() FROM bf_l l, bf_r r WHERE l.x < r.x AND l.y > r.y) + (SELECT count() FROM bf_l) - (SELECT uniqExact(l.id) FROM bf_l l, bf_r r WHERE l.x < r.x AND l.y > r.y)) AS ok, (SELECT count() FROM bf_l l LEFT JOIN bf_r r ON l.x < r.x AND l.y > r.y) AS cnt;

-- SEMI/ANTI: a NaN-keyed left row never matches, so SEMI drops it and ANTI keeps it.
SELECT 'semi', (SELECT arraySort(groupArray(l.id)) FROM bf_l l LEFT SEMI JOIN bf_r r ON l.x < r.x AND l.y > r.y) = (SELECT arraySort(groupArrayDistinct(l.id)) FROM bf_l l, bf_r r WHERE l.x < r.x AND l.y > r.y) AS ok, (SELECT count() FROM bf_l l LEFT SEMI JOIN bf_r r ON l.x < r.x AND l.y > r.y) AS cnt;
SELECT 'anti', (SELECT arraySort(groupArray(l.id)) FROM bf_l l LEFT ANTI JOIN bf_r r ON l.x < r.x AND l.y > r.y) = (SELECT arraySort(groupArray(id)) FROM bf_l WHERE id NOT IN (SELECT l.id FROM bf_l l, bf_r r WHERE l.x < r.x AND l.y > r.y)) AS ok, (SELECT count() FROM bf_l l LEFT ANTI JOIN bf_r r ON l.x < r.x AND l.y > r.y) AS cnt;

DROP TABLE bf_l;
DROP TABLE bf_r;
