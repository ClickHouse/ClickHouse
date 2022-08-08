set optimize_sorting_for_input_stream=1;
DROP TABLE IF EXISTS optimize_sorting;
CREATE TABLE optimize_sorting (a UInt64, b UInt64, c UInt64) ENGINE MergeTree() ORDER BY (a, b);
INSERT INTO optimize_sorting SELECT number, number%5, number%2 from numbers(0, 5);
INSERT INTO optimize_sorting SELECT number, number%5, number%2 from numbers(5, 5);
-- { echoOn }
SELECT a from optimize_sorting order by a;
-- queries with unary function in order by
SELECT a from optimize_sorting order by -a;
SELECT a from optimize_sorting order by toFloat64(a);
-- queries with non-unary function in order by
SELECT a from optimize_sorting order by a+1;
SELECT a from optimize_sorting order by a-1;
SELECT a from optimize_sorting order by sipHash64(a, 'a');
-- queries with aliases
SELECT a as a from optimize_sorting order by a;
SELECT a+1 as a from optimize_sorting order by a;
SELECT toFloat64(a) as a from optimize_sorting order by a;
SELECT sipHash64(a) as a from optimize_sorting order by a;
-- queries with filter+expression
SELECT a FROM optimize_sorting WHERE a > 0 ORDER BY a;
SELECT a > 0 FROM optimize_sorting WHERE a > 0;
SELECT a FROM (SELECT a FROM optimize_sorting) WHERE a != 0 ORDER BY a;
SELECT a FROM (SELECT sipHash64(a) AS a FROM optimize_sorting) WHERE a != 0 ORDER BY a;
-- { echoOff }
-- DROP TABLE IF EXISTS optimize_sorting;
