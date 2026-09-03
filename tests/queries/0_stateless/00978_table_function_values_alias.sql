SELECT x, s, z FROM VALUES('x UInt64, s String, z ALIAS concat(toString(x), \': \', s)', (1, 'hello'), (2, 'world'));
