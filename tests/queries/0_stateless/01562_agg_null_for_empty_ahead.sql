SELECT sumMerge(s) FROM (SELECT sumState(number) s FROM numbers(0));
SELECT sumMerge(s) FROM (SELECT sumState(number) s FROM numbers(1));

SELECT sumMerge(s) FROM (SELECT sumMergeState(n) s FROM (SELECT sumState(number) n FROM numbers(0)));
SELECT sumMerge(s) FROM (SELECT sumMergeState(n) s FROM (SELECT sumState(number) n FROM numbers(1)));

SELECT sumIf(1, 0);

SELECT sumIf(1, 1);

-- should return Null even if we donn't set aggregate_functions_null_for_empty
SELECT sumIfOrNull(1, 0);
SELECT sumOrNullIf(1, 0);

SELECT nullIf(1, 0);

SELECT nullIf(1, 1);

SET aggregate_functions_null_for_empty=1;

SELECT sumMerge(s) FROM (SELECT sumState(number) s FROM numbers(0));
SELECT sumMerge(s) FROM (SELECT sumState(number) s FROM numbers(1));

SELECT sumMerge(s) FROM (SELECT sumMergeState(n) s FROM (SELECT sumState(number) n FROM numbers(0)));
SELECT sumMerge(s) FROM (SELECT sumMergeState(n) s FROM (SELECT sumState(number) n FROM numbers(1)));

SELECT sumIf(1, 0);

SELECT sumIf(1, 1);

SELECT sumIfOrNull(1, 0);
SELECT sumOrNullIf(1, 0);

SELECT nullIf(1, 0);

SELECT nullIf(1, 1);
