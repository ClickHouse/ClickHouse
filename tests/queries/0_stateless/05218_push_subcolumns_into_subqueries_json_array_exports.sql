-- Tags: no-parallel-replicas, no-random-settings
-- The test checks EXPLAIN output, which differs with parallel replicas and randomized plan-related settings.
-- The plan lines starting from `ReadFromMergeTree` are printed: the columns read from the table are its header.

-- The chained `Dynamic`/JSON-array access `x[1].b` where the subquery exports the `Dynamic` value
-- itself (`SELECT json.a AS x`): the nested subcolumn ``json.a.:`Array(JSON)`.b`` is recovered from
-- the projection expression of the export. And the mixed usage `x.a[1].b, x.a`: the nested field is
-- not pushed next to the already pushed `json.a` it is extracted from (`FunctionToSubcolumnsPass`
-- refuses the same mixed usage of a column read directly from a table).

SET enable_analyzer = 1;
SET optimize_push_subcolumns_into_subqueries = 1;

DROP TABLE IF EXISTS t_push_subcolumns_json_exports;

CREATE TABLE t_push_subcolumns_json_exports
(
    id UInt32,
    json JSON,
    d Dynamic
)
ENGINE = MergeTree ORDER BY id;

INSERT INTO t_push_subcolumns_json_exports VALUES (1, '{"a": [{"b": 1, "c": {"e": 10}}, {"b": 2, "c": {"e": 20}}], "z": 100}', [1, 2, 3]);
INSERT INTO t_push_subcolumns_json_exports VALUES (2, '{"a": [{"b": 3, "c": {"e": 30}}], "z": 200}', [4]);

SELECT 'exported Dynamic path: one field';
SELECT arrayJoin(arrayFilter(x -> x NOT LIKE '%Read type%' AND x NOT LIKE '%Parts:%' AND x NOT LIKE '%Output:%', arrayMap(x -> trimLeft(x), arraySlice(lines, arrayFirstIndex(x -> x LIKE '%ReadFromMergeTree%', lines))))) FROM
(
    SELECT groupArray(explain) AS lines FROM (EXPLAIN header = 1 SELECT x[1].b FROM (SELECT json.a AS x FROM t_push_subcolumns_json_exports))
);
SELECT toString(x[1].b) AS v FROM (SELECT json.a AS x FROM t_push_subcolumns_json_exports) ORDER BY v;

SELECT 'exported Dynamic path: nested fields';
SELECT arrayJoin(arrayFilter(x -> x NOT LIKE '%Read type%' AND x NOT LIKE '%Parts:%' AND x NOT LIKE '%Output:%', arrayMap(x -> trimLeft(x), arraySlice(lines, arrayFirstIndex(x -> x LIKE '%ReadFromMergeTree%', lines))))) FROM
(
    SELECT groupArray(explain) AS lines FROM (EXPLAIN header = 1 SELECT x[1].c.e FROM (SELECT json.a AS x FROM t_push_subcolumns_json_exports))
);
SELECT toString(x[1].c.e) AS v FROM (SELECT json.a AS x FROM t_push_subcolumns_json_exports) ORDER BY v;

SELECT 'exported Dynamic path: two fields of the same value';
SELECT arrayJoin(arrayFilter(x -> x NOT LIKE '%Read type%' AND x NOT LIKE '%Parts:%' AND x NOT LIKE '%Output:%', arrayMap(x -> trimLeft(x), arraySlice(lines, arrayFirstIndex(x -> x LIKE '%ReadFromMergeTree%', lines))))) FROM
(
    SELECT groupArray(explain) AS lines FROM (EXPLAIN header = 1 SELECT x[1].b, x[2].c.e FROM (SELECT json.a AS x FROM t_push_subcolumns_json_exports))
);
SELECT toString(x[1].b) AS v1, toString(x[2].c.e) AS v2 FROM (SELECT json.a AS x FROM t_push_subcolumns_json_exports) ORDER BY v1, v2;

SELECT 'exported Dynamic path: through a CTE';
SELECT arrayJoin(arrayFilter(x -> x NOT LIKE '%Read type%' AND x NOT LIKE '%Parts:%' AND x NOT LIKE '%Output:%', arrayMap(x -> trimLeft(x), arraySlice(lines, arrayFirstIndex(x -> x LIKE '%ReadFromMergeTree%', lines))))) FROM
(
    SELECT groupArray(explain) AS lines FROM (EXPLAIN header = 1 WITH cte AS (SELECT json.a AS x FROM t_push_subcolumns_json_exports) SELECT x[1].b FROM cte)
);
WITH cte AS (SELECT json.a AS x FROM t_push_subcolumns_json_exports) SELECT toString(x[1].b) AS v FROM cte ORDER BY v;

SELECT 'exported Dynamic path: UNION ALL';
SELECT arrayJoin(arrayFilter(x -> x NOT LIKE '%Read type%' AND x NOT LIKE '%Parts:%' AND x NOT LIKE '%Output:%', arrayMap(x -> trimLeft(x), arraySlice(lines, arrayFirstIndex(x -> x LIKE '%ReadFromMergeTree%', lines))))) FROM
(
    SELECT groupArray(explain) AS lines FROM (EXPLAIN header = 1 SELECT x[1].b FROM
    (
        SELECT json.a AS x FROM t_push_subcolumns_json_exports
        UNION ALL
        SELECT json.a AS x FROM t_push_subcolumns_json_exports
    ))
);
SELECT toString(x[1].b) AS v FROM
(
    SELECT json.a AS x FROM t_push_subcolumns_json_exports
    UNION ALL
    SELECT json.a AS x FROM t_push_subcolumns_json_exports
) ORDER BY v;

SELECT 'exported Dynamic path derived from a JSON column of a deeper subquery';
SELECT arrayJoin(arrayFilter(x -> x NOT LIKE '%Read type%' AND x NOT LIKE '%Parts:%' AND x NOT LIKE '%Output:%', arrayMap(x -> trimLeft(x), arraySlice(lines, arrayFirstIndex(x -> x LIKE '%ReadFromMergeTree%', lines))))) FROM
(
    SELECT groupArray(explain) AS lines FROM (EXPLAIN header = 1 SELECT y[1].b FROM (SELECT x.a AS y FROM (SELECT json AS x FROM t_push_subcolumns_json_exports)))
);
SELECT toString(y[1].b) AS v FROM (SELECT x.a AS y FROM (SELECT json AS x FROM t_push_subcolumns_json_exports)) ORDER BY v;

SELECT 'a Dynamic value re-exported as a bare column has no subcolumn expression at the intermediate boundary';
SELECT arrayJoin(arrayFilter(x -> x NOT LIKE '%Read type%' AND x NOT LIKE '%Parts:%' AND x NOT LIKE '%Output:%', arrayMap(x -> trimLeft(x), arraySlice(lines, arrayFirstIndex(x -> x LIKE '%ReadFromMergeTree%', lines))))) FROM
(
    SELECT groupArray(explain) AS lines FROM (EXPLAIN header = 1 SELECT y[1].b FROM (SELECT x AS y FROM (SELECT json.a AS x FROM t_push_subcolumns_json_exports)))
);
SELECT toString(y[1].b) AS v FROM (SELECT x AS y FROM (SELECT json.a AS x FROM t_push_subcolumns_json_exports)) ORDER BY v;

SELECT 'a Dynamic column that is not a JSON path is not rewritten';
SELECT arrayJoin(arrayFilter(x -> x NOT LIKE '%Read type%' AND x NOT LIKE '%Parts:%' AND x NOT LIKE '%Output:%', arrayMap(x -> trimLeft(x), arraySlice(lines, arrayFirstIndex(x -> x LIKE '%ReadFromMergeTree%', lines))))) FROM
(
    SELECT groupArray(explain) AS lines FROM (EXPLAIN header = 1 SELECT x[1].b FROM (SELECT d AS x FROM t_push_subcolumns_json_exports))
);

SELECT 'the exported Dynamic value stays alive';
SELECT arrayJoin(arrayFilter(x -> x NOT LIKE '%Read type%' AND x NOT LIKE '%Parts:%' AND x NOT LIKE '%Output:%', arrayMap(x -> trimLeft(x), arraySlice(lines, arrayFirstIndex(x -> x LIKE '%ReadFromMergeTree%', lines))))) FROM
(
    SELECT groupArray(explain) AS lines FROM (EXPLAIN header = 1 SELECT x[1].b, x FROM (SELECT json.a AS x FROM t_push_subcolumns_json_exports))
);

SELECT 'mixed usage: the ancestor subcolumn is pushed, the nested field is extracted from it';
SELECT arrayJoin(arrayFilter(x -> x NOT LIKE '%Read type%' AND x NOT LIKE '%Parts:%' AND x NOT LIKE '%Output:%', arrayMap(x -> trimLeft(x), arraySlice(lines, arrayFirstIndex(x -> x LIKE '%ReadFromMergeTree%', lines))))) FROM
(
    SELECT groupArray(explain) AS lines FROM (EXPLAIN header = 1 SELECT x.a[1].b, x.a FROM (SELECT json AS x FROM t_push_subcolumns_json_exports))
);
SELECT toString(x.a[1].b) AS v1, toString(x.a) AS v2 FROM (SELECT json AS x FROM t_push_subcolumns_json_exports) ORDER BY v1;

SELECT 'mixed usage through a derived export of the ancestor subcolumn';
SELECT arrayJoin(arrayFilter(x -> x NOT LIKE '%Read type%' AND x NOT LIKE '%Parts:%' AND x NOT LIKE '%Output:%', arrayMap(x -> trimLeft(x), arraySlice(lines, arrayFirstIndex(x -> x LIKE '%ReadFromMergeTree%', lines))))) FROM
(
    SELECT groupArray(explain) AS lines FROM (EXPLAIN header = 1 SELECT x.a[1].b, y FROM (SELECT json AS x, json.a AS y FROM t_push_subcolumns_json_exports))
);
SELECT toString(x.a[1].b) AS v1, toString(y) AS v2 FROM (SELECT json AS x, json.a AS y FROM t_push_subcolumns_json_exports) ORDER BY v1;

SELECT 'mixed usage: two nested fields of the same ancestor';
SELECT arrayJoin(arrayFilter(x -> x NOT LIKE '%Read type%' AND x NOT LIKE '%Parts:%' AND x NOT LIKE '%Output:%', arrayMap(x -> trimLeft(x), arraySlice(lines, arrayFirstIndex(x -> x LIKE '%ReadFromMergeTree%', lines))))) FROM
(
    SELECT groupArray(explain) AS lines FROM (EXPLAIN header = 1 SELECT x.a[1].b, x.a[2].c.e, x.a FROM (SELECT json AS x FROM t_push_subcolumns_json_exports))
);

SELECT 'a sibling subcolumn does not block the nested field';
SELECT arrayJoin(arrayFilter(x -> x NOT LIKE '%Read type%' AND x NOT LIKE '%Parts:%' AND x NOT LIKE '%Output:%', arrayMap(x -> trimLeft(x), arraySlice(lines, arrayFirstIndex(x -> x LIKE '%ReadFromMergeTree%', lines))))) FROM
(
    SELECT groupArray(explain) AS lines FROM (EXPLAIN header = 1 SELECT x.a[1].b, x.z FROM (SELECT json AS x FROM t_push_subcolumns_json_exports))
);
SELECT toString(x.a[1].b) AS v1, toString(x.z) AS v2 FROM (SELECT json AS x FROM t_push_subcolumns_json_exports) ORDER BY v1;

SELECT 'setting off';
SELECT arrayJoin(arrayFilter(x -> x NOT LIKE '%Read type%' AND x NOT LIKE '%Parts:%' AND x NOT LIKE '%Output:%', arrayMap(x -> trimLeft(x), arraySlice(lines, arrayFirstIndex(x -> x LIKE '%ReadFromMergeTree%', lines))))) FROM
(
    SELECT groupArray(explain) AS lines FROM (EXPLAIN header = 1 SELECT x[1].b FROM (SELECT json.a AS x FROM t_push_subcolumns_json_exports)
    SETTINGS optimize_push_subcolumns_into_subqueries = 0)
);

DROP TABLE t_push_subcolumns_json_exports;
