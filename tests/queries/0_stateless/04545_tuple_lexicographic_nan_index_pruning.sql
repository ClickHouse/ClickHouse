DROP TABLE IF EXISTS t_tuple_lex_nan;

CREATE TABLE t_tuple_lex_nan (a Float64, b Float64)
ENGINE = MergeTree ORDER BY (a, b) SETTINGS index_granularity = 1;

INSERT INTO t_tuple_lex_nan VALUES (1, 0), (nan, 0);

-- `NaN` does not satisfy ordinary comparisons, so `NOT ((nan, 0) > (0, 0))` must be true.
-- The tuple key condition must not prune the `NaN` granule before row-level filtering.
SELECT count() FROM t_tuple_lex_nan WHERE NOT ((a, b) > (0., 0.));
SELECT (SELECT count() FROM t_tuple_lex_nan WHERE NOT ((a, b) > (0., 0.)))
     = (SELECT count() FROM t_tuple_lex_nan WHERE NOT ((a, b) > (0., 0.)) SETTINGS analyze_index_with_tuple_lexicographic_comparison = 0);

DROP TABLE t_tuple_lex_nan;
