-- The SQL standard frame exclusion. The expected values were cross-checked against DuckDB.

DROP TABLE IF EXISTS t_05176;
CREATE TABLE t_05176 (g UInt8, v UInt8) ENGINE = Memory;
INSERT INTO t_05176 VALUES (1, 10), (1, 20), (2, 30), (3, 40), (3, 50);

SELECT 'the four exclusions over a whole partition';
SELECT g, v,
    sum(v) OVER (ORDER BY g ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE NO OTHERS) AS no_others,
    sum(v) OVER (ORDER BY g ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE CURRENT ROW) AS current_row,
    sum(v) OVER (ORDER BY g ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE GROUP) AS group_,
    sum(v) OVER (ORDER BY g ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE TIES) AS ties
FROM t_05176 ORDER BY g, v;

SELECT 'the peers are the ones of the ORDER BY, whatever the frame type';
SELECT g, v,
    sum(v) OVER (ORDER BY g ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE GROUP) AS rows_,
    sum(v) OVER (ORDER BY g RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE GROUP) AS range_,
    sum(v) OVER (ORDER BY g GROUPS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE GROUP) AS groups_
FROM t_05176 ORDER BY g, v;

SELECT 'a moving frame';
SELECT g, v,
    sum(v) OVER (ORDER BY g, v ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING EXCLUDE CURRENT ROW) AS s
FROM t_05176 ORDER BY g, v;

SELECT 'the frame can be emptied by the exclusion';
SELECT g, v,
    sum(v) OVER (ORDER BY g RANGE BETWEEN CURRENT ROW AND CURRENT ROW EXCLUDE GROUP) AS s,
    count() OVER (ORDER BY g RANGE BETWEEN CURRENT ROW AND CURRENT ROW EXCLUDE GROUP) AS c
FROM t_05176 ORDER BY g, v;

SELECT 'with PARTITION BY';
SELECT p, v,
    sum(v) OVER (PARTITION BY p ORDER BY v ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE CURRENT ROW) AS s
FROM (SELECT number % 2 AS p, toUInt8(number) AS v FROM numbers(6)) ORDER BY p, v;

SELECT 'first_value and last_value follow the exclusion';
SELECT v,
    first_value(v) OVER (ORDER BY v ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE CURRENT ROW) AS fv,
    last_value(v) OVER (ORDER BY v ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE CURRENT ROW) AS lv
FROM (SELECT toUInt8(number * 10) AS v FROM numbers(3)) ORDER BY v;

SELECT 'the ranking functions do not look at the frame, so an exclusion leaves them alone';
SELECT v,
    rank() OVER (ORDER BY v RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE GROUP) AS r,
    dense_rank() OVER (ORDER BY v RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE TIES) AS dr,
    row_number() OVER (ORDER BY v RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE CURRENT ROW) AS rn
FROM (SELECT toUInt8(number % 2) AS v FROM numbers(3)) ORDER BY v, rn;

SELECT 'ntile does not look at the frame either, so an exclusion leaves it alone';
SELECT v,
    ntile(2) OVER (ORDER BY v ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE CURRENT ROW) AS current_row,
    ntile(2) OVER (ORDER BY v ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE GROUP) AS group_,
    ntile(2) OVER (ORDER BY v ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS plain
FROM (SELECT toUInt8(number % 3) AS v FROM numbers(5)) ORDER BY v, plain;

SELECT 'percent_rank and cume_dist require the default frame, and an exclusion is not a change to it';
SELECT v,
    percent_rank() OVER (ORDER BY v RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE CURRENT ROW) AS pr,
    cume_dist() OVER (ORDER BY v RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE GROUP) AS cd,
    percent_rank() OVER (ORDER BY v) AS pr_plain,
    cume_dist() OVER (ORDER BY v) AS cd_plain
FROM (SELECT toUInt8(number % 3) AS v FROM numbers(4)) ORDER BY v, pr;

SELECT 'it holds across block boundaries';
-- `max_block_size` is pinned rather than left to the randomization of the test runner, so that the
-- partition is spread over many blocks whatever the runner picks, and so that the row count can stay
-- small: the exclusion rebuilds the aggregate state per row, which is quadratic over one partition.
WITH t AS (SELECT number AS i, toInt64(number % 101) AS v FROM numbers(20000))
SELECT countIf(s = (SELECT sum(v) FROM t) - v) = count()
FROM (SELECT v, sum(v) OVER (ORDER BY i ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE CURRENT ROW) AS s FROM t)
SETTINGS max_block_size = 1000;

SELECT 'two windows that differ only by the exclusion are two windows';
-- Without the exclusion in the name they share a projection name, and the second answers with the
-- result of the first. The columns are deliberately left unaliased, which is when that happens.
SELECT
    sum(v) OVER (ORDER BY v ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING),
    sum(v) OVER (ORDER BY v ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE CURRENT ROW),
    sum(v) OVER (ORDER BY v RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE GROUP)
FROM t_05176 ORDER BY ALL;

SELECT 'an exclusion that takes nothing out leaves the frame, and the state, alone';
-- The current row is not in either frame to begin with, and the peer group of a unique ORDER BY key
-- holds only the current row, which TIES keeps. Both answer as the same frame without the clause,
-- and both keep the incremental path rather than rebuilding the state per row.
SELECT groupArray(a) = groupArray(b) FROM (
    SELECT sum(v) OVER (ORDER BY g, v ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING EXCLUDE CURRENT ROW) AS a,
           sum(v) OVER (ORDER BY g, v ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS b
    FROM t_05176);
SELECT groupArray(a) = groupArray(b) FROM (
    SELECT sum(v) OVER (ORDER BY v ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW EXCLUDE TIES) AS a,
           sum(v) OVER (ORDER BY v ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS b
    FROM t_05176);
-- A row where the exclusion does take something out, mixed in with rows where it does not.
SELECT v, sum(v) OVER (ORDER BY g ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW EXCLUDE GROUP) FROM t_05176 ORDER BY ALL;

SELECT 'the clause survives formatting';
SELECT formatQuery('SELECT sum(v) OVER (ORDER BY v ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE GROUP) FROM t');
SELECT formatQuery('SELECT sum(v) OVER w FROM t WINDOW w AS (ORDER BY v RANGE BETWEEN 1 PRECEDING AND CURRENT ROW EXCLUDE TIES)');
-- NO OTHERS is the default, so it is not printed back
SELECT formatQuery('SELECT sum(v) OVER (ORDER BY v ROWS UNBOUNDED PRECEDING EXCLUDE NO OTHERS) FROM t');

SELECT 'the clause survives an AST JSON round trip, and an older payload without it still reads';
SELECT formatQueryFromJSON(parseQueryToJSON('SELECT sum(v) OVER (ORDER BY v ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE GROUP) FROM t')) LIKE '%EXCLUDE GROUP%';
SELECT formatQueryFromJSON(parseQueryToJSON('SELECT sum(v) OVER w FROM t WINDOW w AS (ORDER BY v RANGE BETWEEN 1 PRECEDING AND CURRENT ROW EXCLUDE TIES)')) LIKE '%EXCLUDE TIES%';
-- A frame written before the exclusion existed carries no `frame_exclusion`, which reads as `NO OTHERS`.
-- The payload is compared with the one it was cut from, so that a cut that matched nothing fails here
-- rather than passing for the wrong reason.
WITH parseQueryToJSON('SELECT sum(v) OVER (ORDER BY v ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM t') AS json,
     replaceOne(json, ',"frame_exclusion":"NoOthers"', '') AS older
SELECT older != json AND formatQueryFromJSON(older) NOT LIKE '%EXCLUDE%';
-- An exclusion on a window that takes the default frame is not a frame the formatter could print back.
SELECT formatQueryFromJSON(replaceOne(parseQueryToJSON('SELECT sum(v) OVER (ORDER BY v) FROM t'), '"type":"WindowDefinition",', '"type":"WindowDefinition","frame_exclusion":"Ties",')); -- { serverError BAD_ARGUMENTS }

SELECT 'a function that walks the frame itself rejects the exclusion instead of ignoring it';
SELECT nth_value(v, 1) OVER (ORDER BY v ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE CURRENT ROW) FROM t_05176; -- { serverError NOT_IMPLEMENTED }
SELECT lagInFrame(v, 0, 42) OVER (ORDER BY v ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE CURRENT ROW) FROM t_05176; -- { serverError NOT_IMPLEMENTED }
SELECT leadInFrame(v, 1) OVER (ORDER BY v ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE GROUP) FROM t_05176; -- { serverError NOT_IMPLEMENTED }

-- `lag`/`lead` share the implementation with the `InFrame` pair, but they never reach the refusal
-- above: an explicit frame is refused to them by name, so one carrying an exclusion cannot be
-- written in the first place.
SELECT lag(v) OVER (ORDER BY v ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE CURRENT ROW) FROM t_05176; -- { serverError BAD_ARGUMENTS }
SELECT count() FROM (SELECT lag(v) OVER (ORDER BY v) FROM t_05176);

SELECT 'an aggregate that allocates in an arena rejects the exclusion, because the state is rebuilt per row';
SELECT groupArray(v) OVER (ORDER BY v ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE CURRENT ROW) FROM t_05176; -- { serverError NOT_IMPLEMENTED }
SELECT groupArray(v) OVER (ORDER BY v ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE GROUP) FROM t_05176; -- { serverError NOT_IMPLEMENTED }
-- The arena is what the refusal is about, not the size of the state: an aggregate that owns heap
-- memory of its own takes the clause.
SELECT 'a heap-owning state that does not use the arena takes the exclusion';
SELECT v, uniqExact(v) OVER (ORDER BY g ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE GROUP) FROM t_05176 ORDER BY ALL;

DROP TABLE t_05176;
