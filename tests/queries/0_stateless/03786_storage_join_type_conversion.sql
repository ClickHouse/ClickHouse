SET enable_analyzer = 1;
SET join_algorithm = 'hash'; -- default is now grace_hash; pin hash (StorageJoin RIGHT JOIN uses HashJoin) so the EXPLAIN plan is stable
SET max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0; -- Disable automatic spilling for this test

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
