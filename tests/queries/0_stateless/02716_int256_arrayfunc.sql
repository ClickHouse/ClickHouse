SELECT arrayDifference([toUInt128(1), 3]), toTypeName(arrayDifference([toUInt128(1), 3]));
SELECT arrayDifference([toInt128(1), 3]), toTypeName(arrayDifference([toInt128(1), 3]));
SELECT arrayDifference([toUInt256(1), 3]), toTypeName(arrayDifference([toUInt256(1), 3]));
SELECT arrayDifference([toInt256(1), 3]), toTypeName(arrayDifference([toInt256(1), 3]));

SELECT '---';

SELECT arrayCumSum([toUInt128(1), 2]), toTypeName(arrayCumSum([toUInt128(1), 2]));
SELECT arrayCumSum([toInt128(1), 2]), toTypeName(arrayCumSum([toInt128(1), 2]));
SELECT arrayCumSum([toUInt256(1), 2]), toTypeName(arrayCumSum([toUInt256(1), 2]));
SELECT arrayCumSum([toInt256(1), 2]), toTypeName(arrayCumSum([toInt256(1), 2]));

SELECT arrayCumSum([3, toInt128(1), toInt256(1)]), toTypeName(arrayCumSum([toUInt256(1), toUInt128(1)]));
SELECT arrayCumSum([toInt256(1), toInt128(1)]), toTypeName(arrayCumSum([toInt256(1), toInt128(1)]));

SELECT '---';

SELECT arrayCumSumNonNegative([toUInt128(1), 2]), toTypeName(arrayCumSumNonNegative([toUInt128(1), 2]));
SELECT arrayCumSumNonNegative([toInt128(1), -2]), toTypeName(arrayCumSumNonNegative([toInt128(1), -2]));
SELECT arrayCumSumNonNegative([toUInt256(1), 2]), toTypeName(arrayCumSumNonNegative([toUInt256(1), 2]));
SELECT arrayCumSumNonNegative([toInt256(1), -2]), toTypeName(arrayCumSumNonNegative([toInt256(1), -2]));

