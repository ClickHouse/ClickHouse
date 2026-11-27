-- Test for issue #70017: if function type mismatch between UInt64 and Int32
-- The issue was: Unexpected return type from if. Expected Int64. Got Int128.
-- This happened when the canUnsignedBeSigned flag on UInt64 type was lost during
-- query plan transformations in distributed queries.

DROP TABLE IF EXISTS test_if_type_mismatch;

CREATE TABLE test_if_type_mismatch (
    c_f7nvvq Int32,
    c_ooi4e9x212 Float64,
    c_h16pd String,
    c_j3 String,
    c_u37 Float64,
    c_d9n_3 Int32
) ENGINE = MergeTree() ORDER BY c_d9n_3;

INSERT INTO test_if_type_mismatch VALUES (1, 1.5, 'a', 'b', 2.0, 1), (10, 5.5, 'c', 'd', 1.0, 2);

-- Test case 1: Simple if with UInt64 literal (fits in Int64) and Int32 column
SELECT toTypeName(if(rand() % 2, floor(4373163444658715090), c_f7nvvq)) FROM test_if_type_mismatch LIMIT 1;

-- Test case 2: Case expression (which uses if internally)
SELECT toTypeName(
  CASE
    WHEN c_ooi4e9x212 > 5 THEN floor(4373163444658715090)
    ELSE c_f7nvvq
  END
) FROM test_if_type_mismatch LIMIT 1;

-- Test case 3: Query pattern from original issue #70017 using remote() to simulate distributed query
-- This triggers prewhere optimization which materializes the constant and loses the type flag
SELECT
  subq_0.c_q9e4c4y15 as c_n_2010
FROM
  (SELECT
        ref_0.c_ooi4e9x212 as c_or3kcz,
        floor(ref_0.c_f7nvvq) as c_pfnd1iaw,
        floor(4373163444658715090) as c_j59,
        ref_0.c_u37 as c_f_2008,
        ref_0.c_h16pd as c_y2_2009,
        ref_0.c_j3 as c_q9e4c4y15
      FROM
        remote('127.0.0.1', currentDatabase(), test_if_type_mismatch) as ref_0
      WHERE (cast((ref_0.c_u37 > ref_0.c_d9n_3) as Nullable(Bool)))) as subq_0
WHERE (cast((negate(
      case when (cast((subq_0.c_j59 = subq_0.c_pfnd1iaw) as Nullable(Bool))) then 0
           else subq_0.c_pfnd1iaw end
        ) = case when (cast((round(subq_0.c_or3kcz) < subq_0.c_j59) as Nullable(Bool))) then subq_0.c_j59 else subq_0.c_pfnd1iaw end
      ) as Nullable(Bool)));

-- Test case 4: Verify the actual value computation works correctly
SELECT
  if(1, floor(4373163444658715090), toInt32(0)) as when_true,
  if(0, floor(4373163444658715090), toInt32(42)) as when_false;

DROP TABLE test_if_type_mismatch;
