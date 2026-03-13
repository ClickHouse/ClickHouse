-- Tags: no-replicated-database, no-parallel-replicas, no-parallel, no-random-merge-tree-settings
-- EXPLAIN output may differ

-- { echoOn }

DROP TABLE IF EXISTS table_basic;

CREATE TABLE table_basic
(
    d DateTime('America/New_York')
)
ENGINE = MergeTree
ORDER BY d::String
SETTINGS index_granularity = 1;

INSERT INTO table_basic VALUES
    (toDateTime(1730611800, 'America/New_York')),
    (toDateTime(1730615400, 'America/New_York'));

EXPLAIN indexes = 1
SELECT
    toUnixTimestamp(d)
FROM table_basic
WHERE has([toDateTime(1730611800, 'America/New_York')], d);

EXPLAIN indexes = 1
SELECT
    toUnixTimestamp(d)
FROM table_basic
WHERE d IN [toDateTime(1730611800, 'America/New_York')];

EXPLAIN indexes = 1
SELECT
    toUnixTimestamp(d)
FROM table_basic
WHERE has([toDateTime(1730611801, 'America/New_York')], d);

EXPLAIN indexes = 1
SELECT
    toUnixTimestamp(d)
FROM table_basic
WHERE d IN [toDateTime(1730611801, 'America/New_York')];

DROP TABLE IF EXISTS table_intdiv_string;

CREATE TABLE table_intdiv_string
(
    x Int32
)
ENGINE = MergeTree
ORDER BY toString(intDiv(x, 10))
SETTINGS index_granularity = 1;

INSERT INTO table_intdiv_string VALUES
    (2), (5), (9), (10), (12), (15), (19), (20), (29), (33), (39), (40), (55), (59), (90), (95), (99);

EXPLAIN indexes = 1
SELECT arraySort(groupArray(x))
FROM table_intdiv_string
WHERE has(CAST([12,95,2,33,100] AS Array(Int32)), x);

EXPLAIN indexes = 1
SELECT arraySort(groupArray(x))
FROM table_intdiv_string
WHERE x IN CAST([12,95,2,33,100] AS Array(Int32));

EXPLAIN indexes = 1
SELECT arraySort(groupArray(x))
FROM table_intdiv_string
WHERE NOT has(CAST([12,95,2,33,100] AS Array(Int32)), x);

EXPLAIN indexes = 1
SELECT arraySort(groupArray(x))
FROM table_intdiv_string
WHERE x NOT IN CAST([12,95,2,33,100] AS Array(Int32));

DROP TABLE IF EXISTS table_str_chain;

CREATE TABLE table_str_chain
(
    s String
)
ENGINE = MergeTree
ORDER BY reverse(lower(s))
SETTINGS index_granularity = 1;

INSERT INTO table_str_chain VALUES
    ('Abc'), ('abc'),
    ('XYZ'), ('xYz'),
    ('Bar'), ('bar'),
    ('Hello'), ('hello'),
    ('World'), ('world'),
    ('AA'), ('aa'),
    ('A'), ('a'),
    ('baz'), ('Qux'), ('qux');

EXPLAIN indexes = 1
SELECT arraySort(groupArray(s))
FROM table_str_chain
WHERE has(['Abc', 'Bar', 'XYZ', 'Hello', 'AA'], s);

EXPLAIN indexes = 1
SELECT arraySort(groupArray(s))
FROM table_str_chain
WHERE s IN ['Abc', 'Bar', 'XYZ', 'Hello', 'AA'];

EXPLAIN indexes = 1
SELECT arraySort(groupArray(s))
FROM table_str_chain
WHERE NOT has(['Abc', 'Bar', 'XYZ', 'Hello', 'AA'], s);

EXPLAIN indexes = 1
SELECT arraySort(groupArray(s))
FROM table_str_chain
WHERE s NOT IN ['Abc', 'Bar', 'XYZ', 'Hello', 'AA'];

SET use_variant_as_common_type=1;

DROP TABLE IF EXISTS table_json;

CREATE TABLE table_json (id UInt64, json JSON(a UInt32)) ENGINE = MergeTree ORDER BY (json.a, json.b::String) SETTINGS index_granularity = 1;

INSERT INTO table_json SELECT number, toJSONString(map('a', number % 2, 'b', 'str_' || number % 3, 'c', range(number % 10))) FROM numbers(4);
INSERT INTO table_json SELECT number, toJSONString(map('a', number % 2 + 4, 'b', 'str_' || number % 3 + 4, 'c', range(number % 10))) FROM numbers(4, 4);

EXPLAIN indexes = 1
SELECT * FROM table_json WHERE json.a = 0 AND has(['str_0', 'str_1'], json.b) ORDER BY id;

EXPLAIN indexes = 1
SELECT * FROM table_json WHERE json.a = 0 AND NOT has(['str_0', 'str_1'], json.b) ORDER BY id;

DROP TABLE IF EXISTS table_json_or_null;
CREATE TABLE table_json_or_null (id UInt64, json JSON(a UInt32))
ENGINE = MergeTree
ORDER BY (json.a, json.b::UInt64)
SETTINGS index_granularity = 1;

INSERT INTO table_json_or_null
SELECT number, toJSONString(map('a', number % 2, 'b', toString(number % 3)))
FROM numbers(8);

EXPLAIN indexes = 1
SELECT *
FROM table_json_or_null
WHERE json.a = 0 AND has(['0', '1'], json.b)
ORDER BY id;

-- Not supported yet
DROP TABLE IF EXISTS table_json_nested_cast;
CREATE TABLE table_json_nested_cast (id UInt64, json JSON(a UInt32))
ENGINE = MergeTree
ORDER BY (json.a, (json.b::String)::UInt64)
SETTINGS index_granularity = 1;

INSERT INTO table_json_nested_cast
SELECT number, toJSONString(map('a', number % 2, 'b', toString(number % 3)))
FROM numbers(8);

EXPLAIN indexes = 1
SELECT *
FROM table_json_nested_cast
WHERE json.a = 0 AND has(['0', '1'], json.b)
ORDER BY id;
