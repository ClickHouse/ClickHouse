-- https://www.philipzucker.com/sql_graph_csp/

set enable_analyzer = 1;

WITH RECURSIVE digits AS (
    SELECT 0 AS digit
    UNION ALL
    SELECT digit + 1
    FROM digits
    WHERE digit < 9
)
SELECT s.digit AS S, e.digit AS E, n.digit AS N, d.digit AS D,
       m.digit AS M, o.digit AS O, r.digit AS R, y.digit AS Y
FROM digits s, digits e, digits n, digits d, digits m, digits o, digits r, digits y
WHERE s.digit <> e.digit AND s.digit <> n.digit AND s.digit <> d.digit AND s.digit <> m.digit AND
      s.digit <> o.digit AND s.digit <> r.digit AND s.digit <> y.digit AND
      e.digit <> n.digit AND e.digit <> d.digit AND e.digit <> m.digit AND
      e.digit <> o.digit AND e.digit <> r.digit AND e.digit <> y.digit AND
      n.digit <> d.digit AND n.digit <> m.digit AND n.digit <> o.digit AND
      n.digit <> r.digit AND n.digit <> y.digit AND
      d.digit <> m.digit AND d.digit <> o.digit AND d.digit <> r.digit AND
      d.digit <> y.digit AND
      m.digit <> o.digit AND m.digit <> r.digit AND m.digit <> y.digit AND
      o.digit <> r.digit AND o.digit <> y.digit AND
      r.digit <> y.digit AND
      s.digit <> 0 AND m.digit <> 0 AND
      (1000 * s.digit + 100 * e.digit + 10 * n.digit + d.digit) +
      (1000 * m.digit + 100 * o.digit + 10 * r.digit + e.digit) =
      (10000 * m.digit + 1000 * o.digit + 100 * n.digit + 10 * e.digit + y.digit);
