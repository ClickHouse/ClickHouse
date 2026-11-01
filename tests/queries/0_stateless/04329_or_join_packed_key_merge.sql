-- A multi-disjunct (OR) join shares one hash-map type across all disjuncts. When the disjuncts pick
-- different packed fixed-key maps (e.g. keys32 for a (UInt16, UInt16) clause and keys64 for a
-- (UInt32, UInt32) clause), the join must use the widest packed map that holds them all rather than
-- downgrading to the generic `hashed` map. This checks the results stay correct against a
-- cross-join + filter oracle for several mixed-width combinations.

-- Multiple ORs over join keys are only supported by the `hash` algorithm.
SET join_algorithm = 'hash';

DROP TABLE IF EXISTS orj_l;
DROP TABLE IF EXISTS orj_r;

CREATE TABLE orj_l (a UInt16, b UInt16, c UInt32, d UInt32, e UInt32, v UInt64) ENGINE = Memory;
CREATE TABLE orj_r (a UInt16, b UInt16, c UInt32, d UInt32, e UInt32, w UInt64) ENGINE = Memory;

-- Kept small: the oracle is a cross join (O(left * right)), and the flaky check runs this test many
-- times under a per-run time limit in the debug build, so a larger product times out.
INSERT INTO orj_l SELECT number % 100, number % 50, number % 1000, number % 500, number % 30, number FROM numbers(1000);
INSERT INTO orj_r SELECT number % 100, number % 50, number % 1000, number % 500, number % 30, number FROM numbers(800);

-- (UInt16, UInt16) clause [keys32] OR (UInt32, UInt32) clause [keys64] -> merged to keys64.
SELECT 'keys32_or_keys64',
    (SELECT (count(), sum(cityHash64(l.v, r.w))) FROM orj_l AS l ALL INNER JOIN orj_r AS r ON (l.a = r.a AND l.b = r.b) OR (l.c = r.c AND l.d = r.d))
  = (SELECT (count(), sum(cityHash64(l.v, r.w))) FROM orj_l AS l, orj_r AS r WHERE (l.a = r.a AND l.b = r.b) OR (l.c = r.c AND l.d = r.d));

-- (UInt16, UInt16) clause [keys32] OR (UInt32, UInt32, UInt32) clause [keys128] -> merged to keys128.
SELECT 'keys32_or_keys128',
    (SELECT (count(), sum(cityHash64(l.v, r.w))) FROM orj_l AS l ALL INNER JOIN orj_r AS r ON (l.a = r.a AND l.b = r.b) OR (l.c = r.c AND l.d = r.d AND l.e = r.e))
  = (SELECT (count(), sum(cityHash64(l.v, r.w))) FROM orj_l AS l, orj_r AS r WHERE (l.a = r.a AND l.b = r.b) OR (l.c = r.c AND l.d = r.d AND l.e = r.e));

-- (UInt32, UInt32) clause [keys64] OR (UInt32, UInt32) clause [keys64] -> stays keys64.
SELECT 'keys64_or_keys64',
    (SELECT (count(), sum(cityHash64(l.v, r.w))) FROM orj_l AS l ALL INNER JOIN orj_r AS r ON (l.c = r.c AND l.d = r.d) OR (l.d = r.d AND l.e = r.e))
  = (SELECT (count(), sum(cityHash64(l.v, r.w))) FROM orj_l AS l, orj_r AS r WHERE (l.c = r.c AND l.d = r.d) OR (l.d = r.d AND l.e = r.e));

DROP TABLE orj_l;
DROP TABLE orj_r;
