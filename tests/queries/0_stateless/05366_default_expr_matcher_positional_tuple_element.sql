-- `x.1` inside a matcher transformer is a positional `tupleElement`, so it works for unnamed tuples.
DROP TABLE IF EXISTS t_05366;
CREATE TABLE t_05366
(
    t Tuple(UInt8, String),
    a Tuple(UInt8, UInt8),
    b UInt8 DEFAULT plus(COLUMNS('^t$') APPLY (x -> x.1), 0),
    c UInt8 ALIAS plus(COLUMNS('^a$') REPLACE (a.2 AS a), 0),
    d UInt8 MATERIALIZED plus(COLUMNS('^t$') APPLY (x -> arrayMap(y -> y.1, [x])[1]), 0)
)
ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_05366 (t, a) VALUES ((5, 'x'), (3, 4));
SELECT b, c, d FROM t_05366 SETTINGS enable_analyzer = 0;
SELECT b, c, d FROM t_05366 SETTINGS enable_analyzer = 1;
DROP TABLE t_05366;
