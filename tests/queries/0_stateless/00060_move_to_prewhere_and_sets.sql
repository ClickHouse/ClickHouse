-- Tags: stateful
SET optimize_move_to_prewhere = 1;
SELECT uniq(URL) FROM test.hits WHERE TraficSourceID IN (7);
