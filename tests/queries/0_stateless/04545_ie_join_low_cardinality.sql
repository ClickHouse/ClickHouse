-- Tags: no-old-analyzer

-- LowCardinality keys: the operator strips LowCardinality before comparisons, so LC(String)
-- takes the generic comparator, LC(Int32) the encoded fast path, and LC(Nullable(Int32))
-- additionally exercises the NULL-mask extraction after the conversion. All verified against
-- the cross-join oracle.

SET join_algorithm = 'direct,parallel_hash,hash,ie_join';
SET cross_to_inner_join_rewrite = 0;
SET allow_suspicious_low_cardinality_types = 1;
SET max_block_size = 128;

DROP TABLE IF EXISTS lc_l;
DROP TABLE IF EXISTS lc_r;

CREATE TABLE lc_l (id Int32, s LowCardinality(String), i LowCardinality(Int32), ni LowCardinality(Nullable(Int32)), y Int32) ENGINE = MergeTree ORDER BY id;
CREATE TABLE lc_r (id Int32, s LowCardinality(String), i LowCardinality(Int32), ni LowCardinality(Nullable(Int32)), y Int32) ENGINE = MergeTree ORDER BY id;

INSERT INTO lc_l SELECT number, repeat(char(65 + number % 5), 1 + number % 3), toInt32(number % 11 - 5), if(number % 7 = 0, NULL, toInt32(number % 9 - 4)), toInt32(number % 6) FROM numbers(500);
INSERT INTO lc_r SELECT number, repeat(char(66 + number % 4), 1 + (number + 1) % 3), toInt32(number % 13 - 6), if(number % 5 = 0, NULL, toInt32(number % 8 - 4)), toInt32(number % 7) FROM numbers(500);

SELECT 'plan lc string', count() > 0 FROM (EXPLAIN SELECT count() FROM lc_l l JOIN lc_r r ON l.s < r.s AND l.y > r.y) WHERE explain LIKE '%IEJoin%';
SELECT 'plan lc int', count() > 0 FROM (EXPLAIN SELECT count() FROM lc_l l JOIN lc_r r ON l.i < r.i AND l.y > r.y) WHERE explain LIKE '%IEJoin%';
SELECT 'plan lc nullable', count() > 0 FROM (EXPLAIN SELECT count() FROM lc_l l JOIN lc_r r ON l.ni < r.ni AND l.y > r.y) WHERE explain LIKE '%IEJoin%';

SELECT 'lc string', (SELECT (count(), sum(cityHash64(l.id, r.id))) FROM lc_l l JOIN lc_r r ON l.s < r.s AND l.y > r.y) = (SELECT (count(), sum(cityHash64(l.id, r.id))) FROM lc_l l, lc_r r WHERE l.s < r.s AND l.y > r.y) AS ok, (SELECT count() FROM lc_l l JOIN lc_r r ON l.s < r.s AND l.y > r.y) AS cnt;
SELECT 'lc int', (SELECT (count(), sum(cityHash64(l.id, r.id))) FROM lc_l l JOIN lc_r r ON l.i < r.i AND l.y > r.y) = (SELECT (count(), sum(cityHash64(l.id, r.id))) FROM lc_l l, lc_r r WHERE l.i < r.i AND l.y > r.y) AS ok, (SELECT count() FROM lc_l l JOIN lc_r r ON l.i < r.i AND l.y > r.y) AS cnt;
SELECT 'lc nullable', (SELECT (count(), sum(cityHash64(l.id, r.id))) FROM lc_l l JOIN lc_r r ON l.ni < r.ni AND l.y > r.y) = (SELECT (count(), sum(cityHash64(l.id, r.id))) FROM lc_l l, lc_r r WHERE l.ni < r.ni AND l.y > r.y) AS ok, (SELECT count() FROM lc_l l JOIN lc_r r ON l.ni < r.ni AND l.y > r.y) AS cnt;
SELECT 'lc both conditions', (SELECT (count(), sum(cityHash64(l.id, r.id))) FROM lc_l l JOIN lc_r r ON l.i < r.i AND l.s > r.s) = (SELECT (count(), sum(cityHash64(l.id, r.id))) FROM lc_l l, lc_r r WHERE l.i < r.i AND l.s > r.s) AS ok, (SELECT count() FROM lc_l l JOIN lc_r r ON l.i < r.i AND l.s > r.s) AS cnt;

-- LEFT with a LowCardinality Nullable key: unmatched rows include the NULL-keyed ones.
SELECT 'lc nullable left', (SELECT count() FROM lc_l l LEFT JOIN lc_r r ON l.ni < r.ni AND l.y > r.y) = (SELECT (SELECT count() FROM lc_l l, lc_r r WHERE l.ni < r.ni AND l.y > r.y) + (SELECT count() FROM lc_l) - (SELECT uniqExact(l.id) FROM lc_l l, lc_r r WHERE l.ni < r.ni AND l.y > r.y)) AS ok, (SELECT count() FROM lc_l l LEFT JOIN lc_r r ON l.ni < r.ni AND l.y > r.y) AS cnt;

DROP TABLE lc_l;
DROP TABLE lc_r;
