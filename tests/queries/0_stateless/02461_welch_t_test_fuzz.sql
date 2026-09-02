DROP TABLE IF EXISTS welch_ttest__fuzz_7;
CREATE TABLE welch_ttest__fuzz_7 (left UInt128, right UInt128) ENGINE = Memory;

INSERT INTO welch_ttest__fuzz_7 VALUES (0.010268, 0), (0.000167, 0), (0.000167, 0), (0.159258, 1), (0.136278, 1), (0.122389, 1);

SELECT roundBankers(welchTTest(left, right).2, 6) from welch_ttest__fuzz_7;
SELECT roundBankers(studentTTest(left, right).2, 6) from welch_ttest__fuzz_7;
