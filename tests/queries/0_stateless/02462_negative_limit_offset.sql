SELECT number FROM numbers(1000000) LIMIT -1;
SELECT number FROM numbers(1000000) LIMIT -70000;
SELECT number FROM numbers(1000000) LIMIT -100 offset -100000;
SELECT number FROM numbers(1000000) LIMIT -100 offset -900000;
SELECT number FROM numbers(1000000) LIMIT 0 offset -900000;
SELECT number FROM numbers(1000000) offset -999990;
SELECT number FROM numbers(1000000) LIMIT -18446744073709550000 offset -999990;

SELECT number FROM numbers(1000000) LIMIT 5 offset -999990; -- { serverError 440 }
SELECT number FROM numbers(1000000) LIMIT -5 offset 999990; -- { serverError 440 }
SELECT number FROM numbers(1000000) LIMIT -18446744073709551615; -- { serverError 440 }
SELECT number FROM numbers(1000000) OFFSET -18446744073709551615; -- { serverError 440 }
