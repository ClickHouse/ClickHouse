SET enable_analyzer = 1;

CREATE TABLE t1__fuzz_0 (`x` Nullable(UInt32), `str` String) ENGINE = Memory;
CREATE TABLE right_join__fuzz_0 (`x` UInt32, `s` String) ENGINE = Join(`ALL`, RIGHT, x);

EXPLAIN actions = 1, header = 1
SELECT
  *
FROM
  t1__fuzz_0 RIGHT JOIN right_join__fuzz_0 USING (x)
QUALIFY x = 1;

SELECT
  *
FROM
  t1__fuzz_0 RIGHT JOIN right_join__fuzz_0 USING (x)
QUALIFY x = 1;
