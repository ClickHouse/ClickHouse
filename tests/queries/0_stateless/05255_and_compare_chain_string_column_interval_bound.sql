-- A chain that equates a String column with a date-like literal and also bounds a Date column by that
-- same literal and by an interval-shifted one must run and return the matching rows: no comparison may
-- be derived between the String column and the DateTime64 bound, which no ordering supports.
-- https://github.com/ClickHouse/ClickHouse/issues/121954

SET optimize_and_compare_chain = 1;

SELECT count()
FROM
(
    SELECT
        toString(toDate('2026-09-01') + (number % 3)) AS c5,
        toDate('2026-09-01') + (number % 3) AS c3
    FROM numbers(100)
)
WHERE (c5 = '2026-09-02') AND (c3 >= '2026-09-02') AND (c3 <= ('2026-09-02' + toIntervalDay(1)));

-- Same answer with the optimization off.
SELECT count()
FROM
(
    SELECT
        toString(toDate('2026-09-01') + (number % 3)) AS c5,
        toDate('2026-09-01') + (number % 3) AS c3
    FROM numbers(100)
)
WHERE (c5 = '2026-09-02') AND (c3 >= '2026-09-02') AND (c3 <= ('2026-09-02' + toIntervalDay(1)))
SETTINGS optimize_and_compare_chain = 0;

-- The interval-shifted upper bound must still exclude rows: these sit one day past it.
SELECT count()
FROM
(
    SELECT
        toString(toDate('2026-09-01') + (number % 3)) AS c5,
        toDate('2026-09-01') + (number % 3) AS c3
    FROM numbers(100)
)
WHERE (c5 = '2026-09-03') AND (c3 >= '2026-09-01') AND (c3 <= ('2026-09-01' + toIntervalDay(1)));

-- Results alone stay equal even if the pass stops deriving anything on this chain, which would make
-- the arms above vacuous: an edge whose comparison domain is declined is dropped from the chain graph
-- (`continue`), so no walk happens and the counts are unchanged. Pin the derivation on the reporter's
-- own chain instead. Adding the same-domain edge `c2 >= c3` gives it one legitimate transitive
-- conjunct, `c2 >= '2026-09-02'`, while the String column stays in its own domain, so the enabled tree
-- holds exactly 3 `greaterOrEquals` nodes against 2 when disabled.
SELECT count() = 3 FROM (EXPLAIN QUERY TREE SELECT count() FROM (SELECT toString(toDate('2026-09-01') + (number % 3)) AS c5, toDate('2026-09-01') + (number % 3) AS c3, toDate('2026-09-01') + (number % 3) + 1 AS c2 FROM numbers(100)) WHERE (c5 = '2026-09-02') AND (c3 >= '2026-09-02') AND (c3 <= ('2026-09-02' + toIntervalDay(1))) AND (c2 >= c3) SETTINGS optimize_and_compare_chain = 1) WHERE explain ILIKE '%function_name: greaterOrEquals,%';
SELECT count() = 2 FROM (EXPLAIN QUERY TREE SELECT count() FROM (SELECT toString(toDate('2026-09-01') + (number % 3)) AS c5, toDate('2026-09-01') + (number % 3) AS c3, toDate('2026-09-01') + (number % 3) + 1 AS c2 FROM numbers(100)) WHERE (c5 = '2026-09-02') AND (c3 >= '2026-09-02') AND (c3 <= ('2026-09-02' + toIntervalDay(1))) AND (c2 >= c3) SETTINGS optimize_and_compare_chain = 0) WHERE explain ILIKE '%function_name: greaterOrEquals,%';
