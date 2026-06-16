SET enable_analyzer = 1;

SELECT cast(tuple(1, 'Value'), 'Tuple(first UInt64, second String)') AS value, value.first, value.second;

SELECT '--';

WITH (x -> x + 1) AS lambda SELECT lambda(1);

WITH (x -> x + 1) AS lambda SELECT lambda.nested(1); -- { serverError UNKNOWN_IDENTIFIER }

SELECT '--';

SELECT * FROM (SELECT 1) AS t1, t1 AS t2;

SELECT '--';

SELECT * FROM t1 AS t2, (SELECT 1) AS t1;

SELECT * FROM (SELECT 1) AS t1, t1.nested AS t2; -- { serverError UNKNOWN_IDENTIFIER }

SELECT * FROM t1.nested AS t2, (SELECT 1) AS t1; -- { serverError UNKNOWN_IDENTIFIER }
