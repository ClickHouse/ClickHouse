-- A composite fixed set key that fits in 4 or 8 bytes now uses the packed keys32 / keys64 set
-- methods instead of collapsing to keys128. This checks results stay correct across the set
-- consumers (IN, NOT IN, DISTINCT, INTERSECT, EXCEPT) for both widths, against independent oracles.
-- Each query prints 1 when the set-based result equals the oracle.

-- The IN / NOT IN oracles use SEMI / ANTI joins, which only the hash algorithm implements.
SET join_algorithm = 'hash';

DROP TABLE IF EXISTS sk_t16;
DROP TABLE IF EXISTS sk_r16;
DROP TABLE IF EXISTS sk_t32;
DROP TABLE IF EXISTS sk_r32;

-- keys32: two UInt16 columns (4 bytes). The right side covers only half the (a, b) pairs, so some
-- rows match and some do not.
CREATE TABLE sk_t16 (a UInt16, b UInt16) ENGINE = Memory;
CREATE TABLE sk_r16 (a UInt16, b UInt16) ENGINE = Memory;
INSERT INTO sk_t16 SELECT number % 1000, (number * 7) % 1000 FROM numbers(20000);
INSERT INTO sk_r16 SELECT number % 1000, (number * 7) % 1000 FROM numbers(20000) WHERE number % 1000 < 500;

SELECT 'in_keys32',        (SELECT count() FROM sk_t16 WHERE (a, b) IN (SELECT a, b FROM sk_r16))     = (SELECT count() FROM sk_t16 SEMI JOIN sk_r16 USING (a, b));
SELECT 'notin_keys32',     (SELECT count() FROM sk_t16 WHERE (a, b) NOT IN (SELECT a, b FROM sk_r16)) = (SELECT count() FROM sk_t16 ANTI JOIN sk_r16 USING (a, b));
SELECT 'distinct_keys32',  (SELECT count() FROM (SELECT DISTINCT a, b FROM sk_t16))                   = (SELECT uniqExact((a, b)) FROM sk_t16);
SELECT 'intersect_keys32', (SELECT count() FROM (SELECT a, b FROM sk_t16 INTERSECT DISTINCT SELECT a, b FROM sk_r16)) = (SELECT uniqExact((a, b)) FROM sk_t16 WHERE (a, b) IN (SELECT a, b FROM sk_r16));
SELECT 'except_keys32',    (SELECT count() FROM (SELECT a, b FROM sk_t16 EXCEPT DISTINCT SELECT a, b FROM sk_r16))    = (SELECT uniqExact((a, b)) FROM sk_t16 WHERE (a, b) NOT IN (SELECT a, b FROM sk_r16));

-- keys64: two UInt32 columns (8 bytes).
CREATE TABLE sk_t32 (a UInt32, b UInt32) ENGINE = Memory;
CREATE TABLE sk_r32 (a UInt32, b UInt32) ENGINE = Memory;
INSERT INTO sk_t32 SELECT number % 1000, (number * 7) % 1000 FROM numbers(20000);
INSERT INTO sk_r32 SELECT number % 1000, (number * 7) % 1000 FROM numbers(20000) WHERE number % 1000 < 500;

SELECT 'in_keys64',        (SELECT count() FROM sk_t32 WHERE (a, b) IN (SELECT a, b FROM sk_r32))     = (SELECT count() FROM sk_t32 SEMI JOIN sk_r32 USING (a, b));
SELECT 'notin_keys64',     (SELECT count() FROM sk_t32 WHERE (a, b) NOT IN (SELECT a, b FROM sk_r32)) = (SELECT count() FROM sk_t32 ANTI JOIN sk_r32 USING (a, b));
SELECT 'distinct_keys64',  (SELECT count() FROM (SELECT DISTINCT a, b FROM sk_t32))                   = (SELECT uniqExact((a, b)) FROM sk_t32);
SELECT 'intersect_keys64', (SELECT count() FROM (SELECT a, b FROM sk_t32 INTERSECT DISTINCT SELECT a, b FROM sk_r32)) = (SELECT uniqExact((a, b)) FROM sk_t32 WHERE (a, b) IN (SELECT a, b FROM sk_r32));
SELECT 'except_keys64',    (SELECT count() FROM (SELECT a, b FROM sk_t32 EXCEPT DISTINCT SELECT a, b FROM sk_r32))    = (SELECT uniqExact((a, b)) FROM sk_t32 WHERE (a, b) NOT IN (SELECT a, b FROM sk_r32));

DROP TABLE sk_t16;
DROP TABLE sk_r16;
DROP TABLE sk_t32;
DROP TABLE sk_r32;
