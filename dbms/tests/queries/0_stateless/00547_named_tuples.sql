SELECT CAST((1, 'Hello') AS Tuple(x UInt64, s String)) AS t, toTypeName(t), t.1, t.2, tupleElement(t, 'x'), tupleElement(t, 's');
