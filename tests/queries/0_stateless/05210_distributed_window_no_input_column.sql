-- Tags: distributed

-- A window function over a multi-shard query is computed on the initiator, so the shard's plan ends at
-- the "Before WINDOW" step and that step's output is the header the shard sends. When the window
-- function needs no column of its input and every table column is consumed before that boundary
-- (WHERE, PREWHERE or JOIN ON), the header carried no column at all. A block with no columns cannot
-- express how many rows it holds, so all rows were lost with no error.

SET enable_analyzer = 1;
SET prefer_localhost_replica = 0;

DROP TABLE IF EXISTS t_window_no_input;
DROP TABLE IF EXISTS t_window_no_input_keys;

CREATE TABLE t_window_no_input (a UInt64, s String) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_window_no_input SELECT number, toString(number) FROM numbers(1000);

CREATE TABLE t_window_no_input_keys (k UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_window_no_input_keys SELECT number FROM numbers(100);

-- Each route below returned no rows at all. Selecting count() observes the row count, so a result
-- that went back to being empty cannot satisfy these assertions.

SELECT 'WHERE only';
SELECT count(), min(c), max(c) FROM (SELECT count(*) OVER () AS c
    FROM remote('127.0.0.1,127.0.0.1', currentDatabase(), t_window_no_input) WHERE a >= 0);

SELECT 'PREWHERE only';
SELECT count(), min(c), max(c) FROM (SELECT count(*) OVER () AS c
    FROM remote('127.0.0.1,127.0.0.1', currentDatabase(), t_window_no_input) PREWHERE a >= 0);

SELECT 'WHERE only, selective';
SELECT count(), max(c) FROM (SELECT count(*) OVER () AS c
    FROM remote('127.0.0.1,127.0.0.1', currentDatabase(), t_window_no_input) WHERE a < 500);

SELECT 'JOIN ON only';
SELECT count(), max(c) FROM (SELECT count(*) OVER () AS c
    FROM remote('127.0.0.1,127.0.0.1', currentDatabase(), t_window_no_input) AS x
    INNER JOIN t_window_no_input_keys AS y ON x.a = y.k);

SELECT 'table expression projecting an unused constant';
SELECT count(), max(c) FROM (SELECT count(*) OVER () AS c
    FROM remote('127.0.0.1,127.0.0.1', view(SELECT 1 AS cst, number AS a FROM numbers(1000)))
    WHERE a >= 0);

SELECT 'row_number';
SELECT count(), max(c) FROM (SELECT row_number() OVER () AS c
    FROM remote('127.0.0.1,127.0.0.1', currentDatabase(), t_window_no_input) WHERE a >= 0);

SELECT 'serialized query plan';
SELECT count(), max(c) FROM (SELECT count(*) OVER () AS c
    FROM remote('127.0.0.1,127.0.0.1', currentDatabase(), t_window_no_input) WHERE a >= 0)
    SETTINGS serialize_query_plan = 1;

SELECT 'ORDER BY with LIMIT on the initiator';
SELECT count(), max(c) FROM (SELECT count(*) OVER () AS c
    FROM remote('127.0.0.1,127.0.0.1', currentDatabase(), t_window_no_input) WHERE a >= 0
    ORDER BY 1 LIMIT 3);

SELECT 'QUALIFY';
SELECT count(), max(c) FROM (SELECT count(*) OVER () AS c
    FROM remote('127.0.0.1,127.0.0.1', currentDatabase(), t_window_no_input) WHERE a >= 0
    QUALIFY c > 0);

-- Controls: these were already correct and must stay byte for byte identical.

SELECT 'control: the filtered column is also selected';
SELECT count(), max(a), max(c) FROM (SELECT a, count(*) OVER () AS c
    FROM remote('127.0.0.1,127.0.0.1', currentDatabase(), t_window_no_input) WHERE a >= 0);

SELECT 'control: single shard';
SELECT count(), max(c) FROM (SELECT count(*) OVER () AS c
    FROM remote('127.0.0.1', currentDatabase(), t_window_no_input) WHERE a >= 0);

SELECT 'control: not distributed';
SELECT count(), max(c) FROM (SELECT count(*) OVER () AS c FROM t_window_no_input WHERE a >= 0);

SELECT 'control: the result keeps exactly one column';
SELECT * FROM (SELECT count(*) OVER () AS c
    FROM remote('127.0.0.1,127.0.0.1', currentDatabase(), t_window_no_input) WHERE a >= 0)
    ORDER BY c LIMIT 1 FORMAT TSVWithNames;

SELECT 'control: a user column may be named like the placeholder';
SELECT count(), max(mk) FROM (SELECT count(*) OVER () AS c, `__row_count_marker` AS mk
    FROM (SELECT a AS `__row_count_marker`
        FROM remote('127.0.0.1,127.0.0.1', currentDatabase(), t_window_no_input) WHERE a >= 0));

-- The placeholder is added to the mergeable-stage header, and only there.

SELECT 'placeholder is in the distributed plan';
SELECT count() > 0 FROM (EXPLAIN header = 1 SELECT count(*) OVER ()
    FROM remote('127.0.0.1,127.0.0.1', currentDatabase(), t_window_no_input) WHERE a >= 0)
WHERE explain ILIKE '%__row_count_marker%';

SELECT 'placeholder is not in the local plan';
SELECT count() FROM (EXPLAIN header = 1 SELECT count(*) OVER () FROM t_window_no_input WHERE a >= 0)
WHERE explain ILIKE '%__row_count_marker%';

SELECT 'placeholder is not added when a column survives the boundary';
SELECT count() FROM (EXPLAIN header = 1 SELECT a, count(*) OVER ()
    FROM remote('127.0.0.1,127.0.0.1', currentDatabase(), t_window_no_input) WHERE a >= 0)
WHERE explain ILIKE '%__row_count_marker%';

DROP TABLE t_window_no_input;
DROP TABLE t_window_no_input_keys;
