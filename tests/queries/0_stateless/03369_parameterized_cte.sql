-- Basic parameterized CTE
WITH t AS (SELECT {v:String} AS value) SELECT * FROM t(v = 'Hello, world!');

-- Multiple parameters
WITH t AS (SELECT {a:UInt32} + {b:UInt32} AS result) SELECT * FROM t(a = 1, b = 2);
