SET enable_analyzer = 1;

WITH ('a', 'b')::Tuple(c1 String, c2 String) AS t
SELECT t.c1, t.c2;

WITH materialize(('a', 'b')::Tuple(c1 String, c2 String)) AS t
SELECT t.c1, t.c2;

WITH (1, ('a', 'b'))::Tuple(c1 UInt64, t1 Tuple(c1 String, c2 String)) AS t
SELECT t.c1, t.t1.c1, t.t1.c2;

WITH materialize((1, ('a', 'b'))::Tuple(c1 UInt64, t1 Tuple(c1 String, c2 String))) AS t
SELECT t.c1, t.t1.c1, t.t1.c2;

WITH [1, 2, 3] AS arr SELECT arr.size0;
WITH materialize([1, 2, 3]) AS arr SELECT arr.size0;

WITH [1, 2, NULL] AS arr SELECT arr.null;
WITH materialize([1, 2, NULL]) AS arr SELECT arr.null;

WITH [[1, 2], [], [3]] AS arr SELECT arr.size0, arr.size1;
WITH materialize([[1, 2], [], [3]]) AS arr SELECT arr.size0, arr.size1;

WITH map('foo', 1, 'bar', 2) AS m SELECT m.keys, m.values;
WITH materialize(map('foo', 1, 'bar', 2)) AS m SELECT m.keys, m.values;
WITH map('foo', 1, 'bar', 2) AS m SELECT m.*;

WITH map('foo', (1, 2), 'bar', (3, 4))::Map(String, Tuple(a UInt64, b UInt64)) AS m
SELECT m.keys, m.values, m.values.a, m.values.b;

WITH materialize(map('foo', (1, 2), 'bar', (3, 4))::Map(String, Tuple(a UInt64, b UInt64))) AS m
SELECT m.keys, m.values, m.values.a, m.values.b;

WITH map('foo', (1, 2), 'bar', (3, 4))::Map(String, Tuple(a UInt64, b UInt64)) AS m
SELECT m.keys, m.values, m.values.*;

WITH materialize(map('foo', (1, 2), 'bar', (3, 4))::Map(String, Tuple(a UInt64, b UInt64))) AS m
SELECT m.keys, m.values, m.values.*;

WITH [1, 2, 3] AS arr SELECT arr.*; -- { serverError UNSUPPORTED_METHOD }

SELECT getSubcolumn([1, 2, 3], 'size0');
SELECT getSubcolumn([1, 2, 3], materialize('size0')); -- { serverError ILLEGAL_COLUMN }
SELECT getSubcolumn([1, 2, 3], 'aaa'); -- { serverError ILLEGAL_COLUMN }
