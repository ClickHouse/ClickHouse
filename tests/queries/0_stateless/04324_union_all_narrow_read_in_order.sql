-- Read-in-order over a SQL UNION must not be broken by `max_streams_for_union_step*` narrowing.
-- Narrowing concatenates streams via `Concat` processors in random order, which would destroy
-- the per-stream sortedness that the FinishSorting `MergingSortedTransform` relies on.
-- When the plan requires sorted union output streams, narrowing must be skipped.

DROP TABLE IF EXISTS t_union_narrow;
CREATE TABLE t_union_narrow (k UInt32) ENGINE = MergeTree ORDER BY k;
SYSTEM STOP MERGES t_union_narrow;   -- keep 16 separate parts -> 16 in-order streams

INSERT INTO t_union_narrow SELECT  0*1000 + number FROM numbers(100);
INSERT INTO t_union_narrow SELECT  1*1000 + number FROM numbers(100);
INSERT INTO t_union_narrow SELECT  2*1000 + number FROM numbers(100);
INSERT INTO t_union_narrow SELECT  3*1000 + number FROM numbers(100);
INSERT INTO t_union_narrow SELECT  4*1000 + number FROM numbers(100);
INSERT INTO t_union_narrow SELECT  5*1000 + number FROM numbers(100);
INSERT INTO t_union_narrow SELECT  6*1000 + number FROM numbers(100);
INSERT INTO t_union_narrow SELECT  7*1000 + number FROM numbers(100);
INSERT INTO t_union_narrow SELECT  8*1000 + number FROM numbers(100);
INSERT INTO t_union_narrow SELECT  9*1000 + number FROM numbers(100);
INSERT INTO t_union_narrow SELECT 10*1000 + number FROM numbers(100);
INSERT INTO t_union_narrow SELECT 11*1000 + number FROM numbers(100);
INSERT INTO t_union_narrow SELECT 12*1000 + number FROM numbers(100);
INSERT INTO t_union_narrow SELECT 13*1000 + number FROM numbers(100);
INSERT INTO t_union_narrow SELECT 14*1000 + number FROM numbers(100);
INSERT INTO t_union_narrow SELECT 15*1000 + number FROM numbers(100);

SELECT 'read-in-order DESC, narrowing enabled';
SELECT
    res = arrayReverseSort(res)                                                  AS is_sorted,
    arrayCount(p -> p.1 < p.2, arrayZip(arrayPopBack(res), arrayPopFront(res)))  AS order_violations
FROM
(
    SELECT groupArray(k) AS res FROM
    (
        WITH A AS ( SELECT k FROM t_union_narrow UNION ALL SELECT k FROM t_union_narrow )
        SELECT k FROM A ORDER BY k DESC LIMIT 3200
    )
)
SETTINGS optimize_read_in_order = 1, read_in_order_use_virtual_row = 0,
         max_threads = 8, max_streams_to_max_threads_ratio = 1,
         max_streams_for_union_step = 2, max_streams_for_union_step_to_max_threads_ratio = 0;

SELECT 'read-in-order ASC, narrowing enabled';
SELECT
    res = arraySort(res)                                                         AS is_sorted,
    arrayCount(p -> p.1 > p.2, arrayZip(arrayPopBack(res), arrayPopFront(res)))  AS order_violations
FROM
(
    SELECT groupArray(k) AS res FROM
    (
        WITH A AS ( SELECT k FROM t_union_narrow UNION ALL SELECT k FROM t_union_narrow )
        SELECT k FROM A ORDER BY k LIMIT 3200
    )
)
SETTINGS optimize_read_in_order = 1, read_in_order_use_virtual_row = 0,
         max_threads = 8, max_streams_to_max_threads_ratio = 1,
         max_streams_for_union_step = 2, max_streams_for_union_step_to_max_threads_ratio = 0;

-- Sortedness advertised through the union by sorted subqueries (`applyOrder` path).
SELECT 'sorted subqueries in union, narrowing enabled';
SELECT
    res = arraySort(res)                                                         AS is_sorted,
    arrayCount(p -> p.1 > p.2, arrayZip(arrayPopBack(res), arrayPopFront(res)))  AS order_violations
FROM
(
    SELECT groupArray(k) AS res FROM
    (
        SELECT k FROM
        (
            SELECT k FROM (SELECT k FROM t_union_narrow ORDER BY k)
            UNION ALL
            SELECT k FROM (SELECT k FROM t_union_narrow ORDER BY k)
        )
        ORDER BY k LIMIT 3200
    )
)
SETTINGS optimize_read_in_order = 1, read_in_order_use_virtual_row = 0,
         max_threads = 8, max_streams_to_max_threads_ratio = 1,
         max_streams_for_union_step = 2, max_streams_for_union_step_to_max_threads_ratio = 0;

-- Control: same query with default narrowing settings.
SELECT 'read-in-order DESC, default narrowing settings';
SELECT
    res = arrayReverseSort(res)                                                  AS is_sorted,
    arrayCount(p -> p.1 < p.2, arrayZip(arrayPopBack(res), arrayPopFront(res)))  AS order_violations
FROM
(
    SELECT groupArray(k) AS res FROM
    (
        WITH A AS ( SELECT k FROM t_union_narrow UNION ALL SELECT k FROM t_union_narrow )
        SELECT k FROM A ORDER BY k DESC LIMIT 3200
    )
)
SETTINGS optimize_read_in_order = 1, read_in_order_use_virtual_row = 0, max_threads = 8;

DROP TABLE t_union_narrow;
