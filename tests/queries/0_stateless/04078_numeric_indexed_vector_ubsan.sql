-- Test for UB in BSINumericIndexedVector pointwise operations with negative and extreme values.
-- Float-to-integer conversion overflow must throw INCORRECT_DATA, not trigger UB.
-- Negative value comparisons must work correctly via two's complement.

DROP TABLE IF EXISTS t_bsi_float;
DROP TABLE IF EXISTS t_bsi_int;

CREATE TABLE t_bsi_float (ds Date, idx UInt32, value Float64) ENGINE = MergeTree() ORDER BY ds;
INSERT INTO t_bsi_float VALUES ('2023-12-26', 1, 1.5), ('2023-12-26', 2, -3.25), ('2023-12-26', 3, 5.0);

CREATE TABLE t_bsi_int (ds Date, idx UInt32, value Int16) ENGINE = MergeTree() ORDER BY ds;
INSERT INTO t_bsi_int VALUES ('2023-12-26', 1, 10), ('2023-12-26', 2, -5), ('2023-12-26', 3, 100);

-- 1. pointwiseAdd with extreme float values should throw, not trigger UB (initializeFromVectorAndValue overflow)
SELECT numericIndexedVectorPointwiseAdd(groupNumericIndexedVectorState(idx, value), 1e30) FROM t_bsi_float; -- { serverError INCORRECT_DATA }
SELECT numericIndexedVectorPointwiseAdd(groupNumericIndexedVectorState(idx, value), -1e30) FROM t_bsi_float; -- { serverError INCORRECT_DATA }

-- 2. pointwiseEqual with negative float values should not trigger UB
SELECT numericIndexedVectorToMap(numericIndexedVectorPointwiseEqual(groupNumericIndexedVectorState(idx, value), toFloat64(-3.25))) FROM t_bsi_float;
SELECT numericIndexedVectorToMap(numericIndexedVectorPointwiseEqual(groupNumericIndexedVectorState(idx, value), toFloat64(0))) FROM t_bsi_float;
SELECT numericIndexedVectorToMap(numericIndexedVectorPointwiseEqual(groupNumericIndexedVectorState(idx, value), toFloat64(999.0))) FROM t_bsi_float;

-- 3. pointwiseEqual with negative integer values should not trigger UB
SELECT numericIndexedVectorToMap(numericIndexedVectorPointwiseEqual(groupNumericIndexedVectorState(idx, value), toInt16(-5))) FROM t_bsi_int;
SELECT numericIndexedVectorToMap(numericIndexedVectorPointwiseEqual(groupNumericIndexedVectorState(idx, value), toInt16(10))) FROM t_bsi_int;

-- 4. pointwiseLessEqual with negative values (calls both pointwiseLess and pointwiseEqual)
SELECT numericIndexedVectorToMap(numericIndexedVectorPointwiseLessEqual(groupNumericIndexedVectorState(idx, value), toFloat64(-3.25))) FROM t_bsi_float;
SELECT numericIndexedVectorToMap(numericIndexedVectorPointwiseLessEqual(groupNumericIndexedVectorState(idx, value), toInt16(-5))) FROM t_bsi_int;

-- 5. pointwiseEqual with extreme float values should return empty result, not trigger UB
SELECT numericIndexedVectorToMap(numericIndexedVectorPointwiseEqual(groupNumericIndexedVectorState(idx, value), toFloat64(1e30))) FROM t_bsi_float;
SELECT numericIndexedVectorToMap(numericIndexedVectorPointwiseEqual(groupNumericIndexedVectorState(idx, value), toFloat64(-1e30))) FROM t_bsi_float;

DROP TABLE t_bsi_float;
DROP TABLE t_bsi_int;
