SET enable_analyzer = 1;
SET max_execution_time = 300;

-- { echoOn }

SELECT arrayMap(x -> x + arrayMap(x -> x + 1, [1])[1], [1,2,3]);

SELECT '--';

SELECT arrayMap(x -> x + arrayMap(x -> 5, [1])[1], [1,2,3]);

SELECT '--';

SELECT 5 AS constant, arrayMap(x -> x + arrayMap(x -> constant, [1])[1], [1,2,3]);

SELECT '--';

SELECT arrayMap(x -> x + arrayMap(x -> x, [1])[1], [1,2,3]);

SELECT '--';

SELECT arrayMap(x -> x + arrayMap(y -> x + y, [1])[1], [1,2,3]);

SELECT '--';

SELECT arrayMap(x -> x + arrayMap(x -> (SELECT 5), [1])[1], [1,2,3]);

SELECT '--';

SELECT (SELECT 5) AS subquery, arrayMap(x -> x + arrayMap(x -> subquery, [1])[1], [1,2,3]);

SELECT '--';

SELECT arrayMap(x -> x + arrayMap(x -> (SELECT 5 UNION DISTINCT SELECT 5), [1])[1], [1,2,3]);

SELECT '--';

SELECT (SELECT 5 UNION DISTINCT SELECT 5) AS subquery, arrayMap(x -> x + arrayMap(x -> subquery, [1])[1], [1,2,3]);

SELECT '--';

WITH x -> toString(x) AS lambda SELECT arrayMap(x -> lambda(x), [1,2,3]);

SELECT '--';

WITH x -> toString(x) AS lambda SELECT arrayMap(x -> arrayMap(y -> concat(lambda(x), '_', lambda(y)), [1,2,3]), [1,2,3]);

SELECT '--';

DROP TABLE IF EXISTS test_table;
CREATE TABLE test_table
(
    id UInt64,
    value String
) ENGINE=TinyLog;

INSERT INTO test_table VALUES (0, 'Value');

SELECT arrayMap(x -> x + arrayMap(x -> id, [1])[1], [1,2,3]) FROM test_table;

SELECT '--';

SELECT arrayMap(x -> x + arrayMap(x -> x + id, [1])[1], [1,2,3]) FROM test_table;

SELECT '--';

SELECT arrayMap(x -> x + arrayMap(y -> x + y + id, [1])[1], [1,2,3]) FROM test_table;

SELECT '--';

SELECT id AS id_alias, arrayMap(x -> x + arrayMap(y -> x + y + id_alias, [1])[1], [1,2,3]) FROM test_table;

SELECT '--';

SELECT arrayMap(x -> x + arrayMap(x -> 5, [1])[1], [1,2,3]) FROM test_table;

SELECT '--';

SELECT 5 AS constant, arrayMap(x -> x + arrayMap(x -> constant, [1])[1], [1,2,3]) FROM test_table;

SELECT '--';

SELECT 5 AS constant, arrayMap(x -> x + arrayMap(x -> x + constant, [1])[1], [1,2,3]) FROM test_table;

SELECT '--';

SELECT 5 AS constant, arrayMap(x -> x + arrayMap(x -> x + id + constant, [1])[1], [1,2,3]) FROM test_table;

SELECT '--';

SELECT 5 AS constant, arrayMap(x -> x + arrayMap(y -> x + y + id + constant, [1])[1], [1,2,3]) FROM test_table;

SELECT '--';

SELECT arrayMap(x -> x + arrayMap(x -> id + (SELECT id FROM test_table), [1])[1], [1,2,3]) FROM test_table;

SELECT '--';

SELECT arrayMap(x -> id + arrayMap(x -> id + (SELECT id FROM test_table), [1])[1], [1,2,3]) FROM test_table;

SELECT '--';

SELECT arrayMap(x -> id + arrayMap(x -> id + (SELECT id FROM test_table UNION DISTINCT SELECT id FROM test_table), [1])[1], [1,2,3]) FROM test_table;

SELECT '--';

WITH x -> toString(id) AS lambda SELECT arrayMap(x -> lambda(x), [1,2,3]) FROM test_table;

SELECT '--';

WITH x -> toString(id) AS lambda SELECT arrayMap(x -> arrayMap(y -> lambda(y), [1,2,3]), [1,2,3]) FROM test_table;

SELECT '--';

WITH x -> toString(id) AS lambda SELECT arrayMap(x -> arrayMap(y -> concat(lambda(x), '_', lambda(y)), [1,2,3]), [1,2,3]) FROM test_table;

SELECT '--';

SELECT arrayMap(x -> concat(concat(concat(concat(concat(toString(id), '___\0_______\0____'), toString(id), concat(concat(toString(id), ''), toString(id)), toString(id)),
    arrayMap(x -> concat(concat(concat(concat(toString(id), ''), toString(id)), toString(id), '___\0_______\0____'), toString(id)) AS lambda, [NULL, inf, 1, 1]),
    concat(toString(id), NULL), toString(id)), toString(id))) AS lambda, [NULL, NULL, 2147483647])
FROM test_table WHERE concat(concat(concat(toString(id), '___\0_______\0____'), toString(id)), concat(toString(id), NULL), toString(id));

SELECT '--';

SELECT arrayMap(x -> splitByChar(toString(id), arrayMap(x -> toString(1), [NULL])), [NULL]) FROM test_table; -- { serverError ILLEGAL_COLUMN };

DROP TABLE test_table;

-- { echoOff }

SELECT
    groupArray(number) AS counts,
    arraySum(arrayMap(x -> (x + 1), counts)) as hello,
    arrayMap(x -> (x / hello), counts) AS res
FROM numbers(1000000) FORMAT Null;

SELECT
  arrayWithConstant(pow(10,5), 1) AS nums,
  arrayMap(x -> x, nums) AS m,
  arrayMap(x -> x + arraySum(m), m) AS res FORMAT Null;
