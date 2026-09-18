-- Regression for the analyzer CNF: `similarTo`/`notSimilarTo` must be present in the analyzer's
-- `pushNotIntoFunction`/`pullNotOutFunctions` inverse maps (src/Analyzer/Passes/CNF.cpp), mirroring
-- the legacy `TreeCNFConverter`. Otherwise, under `enable_analyzer = 1`, `NOT (s SIMILAR TO p)` is
-- not normalized to `notSimilarTo(s, p)`, so constraint-based optimization diverges from the
-- explicit `s NOT SIMILAR TO p` form (which the parser already produces as `notSimilarTo`).

SET convert_query_to_cnf = 1;
SET optimize_using_constraints = 1;
SET optimize_append_index = 0;

DROP TABLE IF EXISTS t_similar_to_cnf;

CREATE TABLE t_similar_to_cnf
(
    s String,
    CONSTRAINT c ASSUME s NOT SIMILAR TO 'a%'
) ENGINE = Memory;

INSERT INTO t_similar_to_cnf VALUES ('xyz');

-- The `ASSUME s NOT SIMILAR TO 'a%'` constraint proves the filter is always true, so the WHERE is
-- removed entirely. Both filter forms must be optimized identically by the analyzer.

SELECT '-- NOT (s SIMILAR TO ...)';
EXPLAIN QUERY TREE SELECT count() FROM t_similar_to_cnf WHERE NOT (s SIMILAR TO 'a%') SETTINGS enable_analyzer = 1;

SELECT '-- s NOT SIMILAR TO ...';
EXPLAIN QUERY TREE SELECT count() FROM t_similar_to_cnf WHERE s NOT SIMILAR TO 'a%' SETTINGS enable_analyzer = 1;

DROP TABLE t_similar_to_cnf;
