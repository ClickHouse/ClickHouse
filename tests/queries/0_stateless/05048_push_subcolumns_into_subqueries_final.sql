-- Tags: no-parallel-replicas, no-random-settings
-- The test checks EXPLAIN output, which differs with parallel replicas and randomized plan-related settings.

-- `FunctionToSubcolumnsPass` refuses to rewrite functions into subcolumn reads under FINAL,
-- because the rewrite may alter the special merging algorithms and produce a wrong result.
-- The subquery form of the same expression must not tunnel around that restriction: a
-- function-form carrier is not pushed down to a direct subcolumn read of a table with FINAL.
-- A plain subcolumn access (`t.a`) is resolved into a direct subcolumn read by the analyzer
-- itself even with FINAL, so its pushdown stays allowed.

SET enable_analyzer = 1;
SET optimize_push_subcolumns_into_subqueries = 1;

DROP TABLE IF EXISTS t_push_subcolumns_final;

CREATE TABLE t_push_subcolumns_final
(
    id UInt32,
    ver UInt32,
    arr Array(UInt32),
    n Nullable(UInt32),
    tup Tuple(a UInt32, b String)
)
ENGINE = ReplacingMergeTree(ver) ORDER BY id;

INSERT INTO t_push_subcolumns_final VALUES (1, 1, [1, 2, 3], 1, (10, 'old')), (2, 1, [], NULL, (20, 'two'));
INSERT INTO t_push_subcolumns_final VALUES (1, 2, [7], NULL, (11, 'new'));

SELECT 'length is not pushed under FINAL';
SELECT trimLeft(explain) FROM
(
    EXPLAIN actions = 1, header = 1 SELECT length(a) FROM (SELECT arr AS a FROM t_push_subcolumns_final FINAL)
) WHERE explain LIKE '%ReadFromMergeTree%' OR explain LIKE '%Header:%';
SELECT length(a) FROM (SELECT arr AS a FROM t_push_subcolumns_final FINAL) ORDER BY 1;

SELECT 'tupleElement is not pushed under FINAL';
SELECT trimLeft(explain) FROM
(
    EXPLAIN actions = 1, header = 1 SELECT tupleElement(x, 1) FROM (SELECT tup AS x FROM t_push_subcolumns_final FINAL)
) WHERE explain LIKE '%ReadFromMergeTree%' OR explain LIKE '%Header:%';
SELECT tupleElement(x, 1) FROM (SELECT tup AS x FROM t_push_subcolumns_final FINAL) ORDER BY 1;

SELECT 'isNotNull is not pushed under FINAL';
SELECT trimLeft(explain) FROM
(
    EXPLAIN actions = 1, header = 1 SELECT isNotNull(x) FROM (SELECT n AS x FROM t_push_subcolumns_final FINAL)
) WHERE explain LIKE '%ReadFromMergeTree%' OR explain LIKE '%Header:%';
SELECT isNotNull(x) FROM (SELECT n AS x FROM t_push_subcolumns_final FINAL) ORDER BY 1;

SELECT 'count of a Nullable is not pushed under FINAL';
SELECT trimLeft(explain) FROM
(
    EXPLAIN actions = 1, header = 1 SELECT count(x) FROM (SELECT n AS x FROM t_push_subcolumns_final FINAL)
) WHERE explain LIKE '%ReadFromMergeTree%' OR explain LIKE '%Header:%';
SELECT count(x) FROM (SELECT n AS x FROM t_push_subcolumns_final FINAL);

SELECT 'a carrier is not pushed to the table through two levels under FINAL';
SELECT trimLeft(explain) FROM
(
    EXPLAIN actions = 1, header = 1 SELECT length(a) FROM (SELECT a FROM (SELECT arr AS a FROM t_push_subcolumns_final FINAL))
) WHERE explain LIKE '%ReadFromMergeTree%' OR explain LIKE '%Header:%';
SELECT length(a) FROM (SELECT a FROM (SELECT arr AS a FROM t_push_subcolumns_final FINAL)) ORDER BY 1;

SELECT 'a plain subcolumn read is pushed under FINAL';
SELECT trimLeft(explain) FROM
(
    EXPLAIN actions = 1, header = 1 SELECT x.a FROM (SELECT tup AS x FROM t_push_subcolumns_final FINAL)
) WHERE explain LIKE '%ReadFromMergeTree%' OR explain LIKE '%Header:%';
SELECT x.a FROM (SELECT tup AS x FROM t_push_subcolumns_final FINAL) ORDER BY 1;

SELECT 'without FINAL the carrier is pushed';
SELECT trimLeft(explain) FROM
(
    EXPLAIN actions = 1, header = 1 SELECT length(a) FROM (SELECT arr AS a FROM t_push_subcolumns_final)
) WHERE explain LIKE '%ReadFromMergeTree%' OR explain LIKE '%Header:%';
SELECT length(a) FROM (SELECT arr AS a FROM t_push_subcolumns_final) ORDER BY 1;

DROP TABLE t_push_subcolumns_final;
