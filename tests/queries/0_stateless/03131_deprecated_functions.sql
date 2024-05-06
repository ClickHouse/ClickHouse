SELECT number, neighbor(number, 2) FROM system.numbers LIMIT 10; -- { serverError 721 }

SELECT runningDifference(number) FROM system.numbers LIMIT 10; -- { serverError 721 }

SELECT k, runningAccumulate(sum_k) AS res FROM (SELECT number as k, sumState(k) AS sum_k FROM numbers(10) GROUP BY k ORDER BY k); -- { serverError 721 }

SET allow_deprecated_functions=1;

SELECT number, neighbor(number, 2) FROM system.numbers LIMIT 10 FORMAT Null;

SELECT runningDifference(number) FROM system.numbers LIMIT 10 FORMAT Null;

SELECT k, runningAccumulate(sum_k) AS res FROM (SELECT number as k, sumState(k) AS sum_k FROM numbers(10) GROUP BY k ORDER BY k) FORMAT Null;
