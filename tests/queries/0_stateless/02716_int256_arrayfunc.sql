SELECT arrayCumSum([CAST('1', 'Int128'), 2]);
SELECT arrayCumSum([CAST('1', 'Int256'), 2]);
SELECT arrayCumSum([CAST('1', 'UInt128'), 2]);
SELECT arrayCumSum([CAST('1', 'UInt256'), 2]);
SELECT arrayCumSum([3, CAST('1', 'Int128'),CAST('1', 'Int256')]);
SELECT arrayCumSum([CAST('1', 'Int256'), CAST('1', 'Int128')]);
SELECT toTypeName(arrayCumSum([CAST('1', 'UInt128'), 2]));
SELECT toTypeName(arrayCumSum([CAST('1', 'Int128'), 2]));
SELECT toTypeName(arrayCumSum([CAST('1', 'UInt256'), CAST('1', 'UInt128')]));
SELECT toTypeName(arrayCumSum([CAST('1', 'Int256'), CAST('1', 'Int128')]));

SELECT arrayDifference([CAST('1', 'Int256'), 3]);
SELECT arrayDifference([CAST('1', 'UInt256'), 3]);
SELECT arrayDifference([CAST('1', 'UInt128'), 3]);
SELECT arrayDifference([CAST('1', 'Int128'), 3]);
SELECT toTypeName(arrayDifference([CAST('1', 'UInt128'), 3]));
SELECT toTypeName(arrayDifference([CAST('1', 'Int128'), 3]));
SELECT toTypeName(arrayDifference([CAST('1', 'UInt256'), 3]));
SELECT toTypeName(arrayDifference([CAST('1', 'Int256'), 3]));