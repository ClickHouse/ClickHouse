-- Tags: no-old-analyzer

-- Residual conditions over LowCardinality columns: the residual result column is
-- LowCardinality(UInt8) (folded by the operator) and the residual inputs are gathered from
-- LC columns. Verified against the hash join executing the same query.

SET join_algorithm = 'ie_join,hash';
SET allow_suspicious_low_cardinality_types = 1;
SET max_block_size = 128;

DROP TABLE IF EXISTS lcr_l;
DROP TABLE IF EXISTS lcr_r;

CREATE TABLE lcr_l (id Int32, x Int32, y Int32, s LowCardinality(String), ns LowCardinality(Nullable(String))) ENGINE = MergeTree ORDER BY id;
CREATE TABLE lcr_r (id Int32, x Int32, y Int32, s LowCardinality(String), ns LowCardinality(Nullable(String))) ENGINE = MergeTree ORDER BY id;

-- `s` values G and H never occur on the right side, so their rows are decided ANTI
INSERT INTO lcr_l SELECT number + 1, toInt32(number % 17), toInt32(100 - number % 23), char(65 + number % 8), if(number % 7 = 0, NULL, char(65 + number % 4)) FROM numbers(300);
INSERT INTO lcr_r SELECT number + 1, toInt32(number % 19 + 2), toInt32(95 - number % 21), char(65 + number % 6), if(number % 5 = 0, NULL, char(65 + number % 3)) FROM numbers(300);

SELECT 'routed', count() > 0 FROM (EXPLAIN actions = 1 SELECT count() FROM lcr_l l LEFT JOIN lcr_r r ON l.x < r.x AND l.y > r.y AND l.s = r.s) WHERE explain LIKE '%IEJoin%';
SELECT 'residual', count() > 0 FROM (EXPLAIN actions = 1 SELECT count() FROM lcr_l l LEFT JOIN lcr_r r ON l.x < r.x AND l.y > r.y AND l.s = r.s) WHERE explain LIKE '%Residual filter%';

SELECT 'left', (
    SELECT (count(), sum(cityHash64(l.id, r.id))) FROM lcr_l l LEFT JOIN lcr_r r ON l.x < r.x AND l.y > r.y AND l.s = r.s
) = (
    SELECT (count(), sum(cityHash64(l.id, r.id))) FROM lcr_l l LEFT JOIN lcr_r r ON l.x < r.x AND l.y > r.y AND l.s = r.s
    SETTINGS join_algorithm = 'hash'
) AS ok, (SELECT count() FROM lcr_l l LEFT JOIN lcr_r r ON l.x < r.x AND l.y > r.y AND l.s = r.s) AS cnt;

SELECT 'semi', (
    SELECT (count(), sum(cityHash64(l.id))) FROM lcr_l l LEFT SEMI JOIN lcr_r r ON l.x < r.x AND l.y > r.y AND l.s = r.s
) = (
    SELECT (count(), sum(cityHash64(l.id))) FROM lcr_l l LEFT SEMI JOIN lcr_r r ON l.x < r.x AND l.y > r.y AND l.s = r.s
    SETTINGS join_algorithm = 'hash'
) AS ok, (SELECT count() FROM lcr_l l LEFT SEMI JOIN lcr_r r ON l.x < r.x AND l.y > r.y AND l.s = r.s) AS cnt;

SELECT 'anti', (
    SELECT (count(), sum(cityHash64(l.id))) FROM lcr_l l LEFT ANTI JOIN lcr_r r ON l.x < r.x AND l.y > r.y AND l.s = r.s
) = (
    SELECT (count(), sum(cityHash64(l.id))) FROM lcr_l l LEFT ANTI JOIN lcr_r r ON l.x < r.x AND l.y > r.y AND l.s = r.s
    SETTINGS join_algorithm = 'hash'
) AS ok, (SELECT count() FROM lcr_l l LEFT ANTI JOIN lcr_r r ON l.x < r.x AND l.y > r.y AND l.s = r.s) AS cnt;

-- A nullable LC residual: pairs where it evaluates to NULL never match
SELECT 'semi nullable', (
    SELECT (count(), sum(cityHash64(l.id))) FROM lcr_l l LEFT SEMI JOIN lcr_r r ON l.x < r.x AND l.y > r.y AND l.ns = r.ns
) = (
    SELECT (count(), sum(cityHash64(l.id))) FROM lcr_l l LEFT SEMI JOIN lcr_r r ON l.x < r.x AND l.y > r.y AND l.ns = r.ns
    SETTINGS join_algorithm = 'hash'
) AS ok, (SELECT count() FROM lcr_l l LEFT SEMI JOIN lcr_r r ON l.x < r.x AND l.y > r.y AND l.ns = r.ns) AS cnt;

DROP TABLE lcr_l;
DROP TABLE lcr_r;
