-- Numeric 1/0 must parse as Bool inside containers via the text-quoted element path,
-- just like the word form true/false. Regression test for issue #109882.
SELECT CAST('[1,0]', 'Array(Bool)');
SELECT CAST('[true,false]', 'Array(Bool)');
SELECT CAST('[true,0,1,false]', 'Array(Bool)');
SELECT CAST('[1,NULL,0]', 'Array(Nullable(Bool))');
SELECT CAST('(1,0)', 'Tuple(Bool,Bool)');
SELECT CAST('(true,false)', 'Tuple(Bool,Bool)');
SELECT [1,0]::Array(Bool);

SELECT * FROM format(CSV, 'x Array(Bool)', '"[1,0]"');
SELECT * FROM format(CSV, 'x Array(Bool)', '"[true,false]"');
SELECT * FROM format(CSV, 'x Array(Bool)', '"[true,0,1,false]"');

-- Map(String, Bool) routes values through the same deserializeTextQuoted helper, so pin it too.
SELECT * FROM format(Values, 'x Map(String, Bool)', $$({'a':1,'b':0})$$);
SELECT * FROM format(Values, 'x Map(String, Bool)', $$({'a':true,'b':false})$$);
SELECT * FROM format(CSV, 'x Map(String, Bool)', '"{''a'':1,''b'':0}"');
SELECT * FROM format(CSV, 'x Map(String, Bool)', '"{''a'':true,''b'':false}"');
