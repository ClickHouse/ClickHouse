-- Test OR-join hash-map merge picks keys256 when a disjunct needs the widest packed map.
SET join_algorithm = 'hash';

DROP TABLE IF EXISTS orj256_l;
DROP TABLE IF EXISTS orj256_r;

CREATE TABLE orj256_l (a UInt16, b UInt16, p UInt64, q UInt64, s UInt64, v UInt64) ENGINE = Memory;
CREATE TABLE orj256_r (a UInt16, b UInt16, p UInt64, q UInt64, s UInt64, w UInt64) ENGINE = Memory;

INSERT INTO orj256_l SELECT number % 100, number % 50, number % 1000, number % 500, number % 30, number FROM numbers(1000);
INSERT INTO orj256_r SELECT number % 100, number % 50, number % 1000, number % 500, number % 30, number FROM numbers(800);

-- Make the keys256 (24-byte) disjunct contribute a match the keys32 disjunct cannot:
-- equal (p, q, s) outside the generated ranges, but different (a, b). Without this the
-- 24-byte clause is a strict subset of (a, b) and the oracle passes even if it is dropped.
INSERT INTO orj256_l VALUES (1, 1, 999999, 888888, 777777, 100000);
INSERT INTO orj256_r VALUES (2, 3, 999999, 888888, 777777, 200000);

-- (UInt16, UInt16) clause [keys32] OR (UInt64, UInt64, UInt64) = 24 bytes clause [keys256] -> merged to keys256.
SELECT 'keys32_or_keys256',
    (SELECT (count(), sum(cityHash64(l.v, r.w))) FROM orj256_l AS l ALL INNER JOIN orj256_r AS r ON (l.a = r.a AND l.b = r.b) OR (l.p = r.p AND l.q = r.q AND l.s = r.s))
  = (SELECT (count(), sum(cityHash64(l.v, r.w))) FROM orj256_l AS l, orj256_r AS r WHERE (l.a = r.a AND l.b = r.b) OR (l.p = r.p AND l.q = r.q AND l.s = r.s));

DROP TABLE orj256_l;
DROP TABLE orj256_r;
