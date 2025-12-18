DROP FUNCTION IF EXISTS explain_lines;
DROP FUNCTION IF EXISTS explain_index_pos;
DROP FUNCTION IF EXISTS explain_index_condition_line;
DROP FUNCTION IF EXISTS explain_index_granules_line;
DROP FUNCTION IF EXISTS explain_index_granules_read;
DROP FUNCTION IF EXISTS explain_index_granules_total;
DROP FUNCTION IF EXISTS explain_index_granules_pruned;
DROP FUNCTION IF EXISTS explain_index;

CREATE FUNCTION explain_lines AS (pairs) ->
    arrayMap(t -> trimLeft(t.2), arraySort(pairs));

CREATE FUNCTION explain_index_pos AS (pairs, idx) ->
    arrayFirstIndex(x -> x = idx, explain_lines(pairs));

CREATE FUNCTION explain_index_condition_line AS (pairs, idx) ->
    arrayFirst(
        x -> startsWith(x, 'Condition:'),
        arraySlice(explain_lines(pairs), explain_index_pos(pairs, idx) + 1)
    );

CREATE FUNCTION explain_index_granules_line AS (pairs, idx) ->
    arrayFirst(
        x -> startsWith(x, 'Granules:'),
        arraySlice(explain_lines(pairs), explain_index_pos(pairs, idx) + 1)
    );

CREATE FUNCTION explain_index_granules_read AS (pairs, idx) ->
    toUInt64OrZero(extract(explain_index_granules_line(pairs, idx), 'Granules: ([0-9]+)'));

CREATE FUNCTION explain_index_granules_total AS (pairs, idx) ->
    toUInt64OrZero(extract(explain_index_granules_line(pairs, idx), 'Granules: [0-9]+/([0-9]+)'));

CREATE FUNCTION explain_index_granules_pruned AS (pairs, idx) ->
    explain_index_granules_read(pairs, idx) < explain_index_granules_total(pairs, idx);

CREATE FUNCTION explain_index AS (pairs, idx) ->
[
    explain_index_condition_line(pairs, idx),
    concat('Granules: ', if(explain_index_granules_pruned(pairs, idx), 'read < total_granules', 'read >= total_granules'))
];

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

SELECT
    toUnixTimestamp(d),
FROM table_basic
WHERE has([toDateTime(1730611800, 'America/New_York')], d);

WITH ( SELECT groupArray((rowNumberInAllBlocks(), explain)) FROM (
    EXPLAIN indexes = 1
    SELECT
        toUnixTimestamp(d),
    FROM table_basic
    WHERE has([toDateTime(1730611800, 'America/New_York')], d)
)) AS plan SELECT arrayJoin(explain_index(plan, 'PrimaryKey')) AS explain;

SELECT
    toUnixTimestamp(d),
FROM table_basic
WHERE not has([toDateTime(1730611800, 'America/New_York')], d);

WITH ( SELECT groupArray((rowNumberInAllBlocks(), explain)) FROM (
    EXPLAIN indexes = 1
    SELECT
        toUnixTimestamp(d),
    FROM table_basic
    WHERE not has([toDateTime(1730611800, 'America/New_York')], d)
)) AS plan SELECT arrayJoin(explain_index(plan, 'PrimaryKey')) AS explain;

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

WITH ( SELECT groupArray((rowNumberInAllBlocks(), explain)) FROM (
    EXPLAIN indexes = 1
    SELECT arraySort(groupArray(x))
    FROM table_intdiv_string
    WHERE has(CAST([12,95,2,33,100] AS Array(Int32)), x)
)) AS plan SELECT arrayJoin(explain_index(plan, 'PrimaryKey')) AS explain;

SELECT arraySort(groupArray(x))
FROM table_intdiv_string
WHERE has(CAST([12,95,2,33,100] AS Array(Int32)), x);

WITH ( SELECT groupArray((rowNumberInAllBlocks(), explain)) FROM (
    EXPLAIN indexes = 1
    SELECT arraySort(groupArray(x))
    FROM table_intdiv_string
    WHERE NOT has(CAST([12,95,2,33,100] AS Array(Int32)), x)
)) AS plan SELECT arrayJoin(explain_index(plan, 'PrimaryKey')) AS explain;

SELECT arraySort(groupArray(x))
FROM table_intdiv_string
WHERE NOT has(CAST([12,95,2,33,100] AS Array(Int32)), x);


DROP TABLE IF EXISTS table_str_chain;

CREATE TABLE table_str_chain
(
    s String
)
ENGINE = MergeTree
ORDER BY reverse(lowerUTF8(s))
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

WITH ( SELECT groupArray((rowNumberInAllBlocks(), explain)) FROM (
    EXPLAIN indexes = 1
    SELECT arraySort(groupArray(s))
    FROM table_str_chain
    WHERE has(['Abc', 'Bar', 'XYZ', 'Hello', 'AA'], s)
)) AS plan SELECT arrayJoin(explain_index(plan, 'PrimaryKey')) AS explain;

SELECT arraySort(groupArray(s))
FROM table_str_chain
WHERE has(['Abc', 'Bar', 'XYZ', 'Hello', 'AA'], s);

WITH ( SELECT groupArray((rowNumberInAllBlocks(), explain)) FROM (
    EXPLAIN indexes = 1
    SELECT arraySort(groupArray(s))
    FROM table_str_chain
    WHERE NOT has(['Abc', 'Bar', 'XYZ', 'Hello', 'AA'], s)
)) AS plan SELECT arrayJoin(explain_index(plan, 'PrimaryKey')) AS explain;

SELECT arraySort(groupArray(s))
FROM table_str_chain
WHERE NOT has(['Abc', 'Bar', 'XYZ', 'Hello', 'AA'], s);

SET use_variant_as_common_type=1;

DROP TABLE IF EXISTS table_json;

CREATE TABLE table_json (id UInt64, json JSON(a UInt32)) ENGINE = MergeTree ORDER BY (json.a, json.b::String) SETTINGS index_granularity = 1;

INSERT INTO table_json SELECT number, toJSONString(map('a', number % 2, 'b', 'str_' || number % 3, 'c', range(number % 10))) FROM numbers(4);
INSERT INTO table_json SELECT number, toJSONString(map('a', number % 2 + 4, 'b', 'str_' || number % 3 + 4, 'c', range(number % 10))) FROM numbers(4, 4);

SELECT * FROM table_json WHERE json.a = 0 AND has(['str_0', 'str_1'], json.b) ORDER BY id;

WITH ( SELECT groupArray((rowNumberInAllBlocks(), explain)) FROM (
    EXPLAIN indexes = 1 SELECT * FROM table_json WHERE json.a = 0 AND has(['str_0', 'str_1'], json.b) ORDER BY id
)) AS plan SELECT arrayJoin(explain_index(plan, 'PrimaryKey')) AS explain;

SELECT * FROM table_json WHERE json.a = 0 AND NOT has(['str_0', 'str_1'], json.b) ORDER BY id;

WITH ( SELECT groupArray((rowNumberInAllBlocks(), explain)) FROM (
    EXPLAIN indexes = 1 SELECT * FROM table_json WHERE json.a = 0 AND NOT has(['str_0', 'str_1'], json.b) ORDER BY id
)) AS plan SELECT arrayJoin(explain_index(plan, 'PrimaryKey')) AS explain;
