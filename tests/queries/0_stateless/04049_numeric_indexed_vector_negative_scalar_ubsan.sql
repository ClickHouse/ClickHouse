-- Float-to-Int64 conversion overflow in initializeFromVectorAndValue should throw INCORRECT_DATA, not trigger UB.

DROP TABLE IF EXISTS t_float;
CREATE TABLE t_float (ds Date, uin UInt32, value Float64) ENGINE = MergeTree() ORDER BY ds;
INSERT INTO t_float VALUES ('2023-12-26', 1, 1.5);

SELECT numericIndexedVectorPointwiseAdd(groupNumericIndexedVectorState(uin, value), 1e30) FROM t_float; -- { serverError INCORRECT_DATA }
SELECT numericIndexedVectorPointwiseAdd(groupNumericIndexedVectorState(uin, value), -1e30) FROM t_float; -- { serverError INCORRECT_DATA }

-- pointwiseEqual with out-of-range float should return empty result, not trigger UB.
SELECT numericIndexedVectorToMap(numericIndexedVectorPointwiseEqual(groupNumericIndexedVectorState(uin, value), 1e30)) FROM t_float;
SELECT numericIndexedVectorToMap(numericIndexedVectorPointwiseEqual(groupNumericIndexedVectorState(uin, value), -1e30)) FROM t_float;

-- pointwiseEqual with a negative scalar that fits in BSI should work correctly.
SELECT numericIndexedVectorToMap(numericIndexedVectorPointwiseEqual(groupNumericIndexedVectorState(uin, value), -3.5)) FROM t_float;

DROP TABLE t_float;

-- pointwiseEqual with total_bit_num == 0 should not trigger UB from shift underflow.
DROP TABLE IF EXISTS t_zero_bits;
CREATE TABLE t_zero_bits (ds Date, uin UInt32, value Float64) ENGINE = MergeTree() ORDER BY ds;
INSERT INTO t_zero_bits VALUES ('2023-12-26', 1, 0);

SELECT numericIndexedVectorToMap(numericIndexedVectorPointwiseEqual(groupNumericIndexedVectorState('BSI', 0, 0)(uin, value), 1.0)) FROM t_zero_bits;

DROP TABLE t_zero_bits;
