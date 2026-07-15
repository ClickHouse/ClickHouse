-- `tid` returns the id of the thread processing the current block, which varies between
-- blocks of one query. The analyzer must not fold it during analysis into the analysis
-- thread's id: the resolved projection must keep the function node instead of replacing
-- it with a constant.

SELECT countIf(explain LIKE '%CONSTANT id%') = 0, countIf(explain LIKE '%function_name: tid%') = 1
FROM (EXPLAIN QUERY TREE run_passes = 1 SELECT tid())
SETTINGS enable_analyzer = 1;
