set optimize_arithmetic_operations_in_aggregate_functions = 1;

explain syntax select min((n as a) + (1 as b)) c from (select number n from numbers(10)) where a > 0 and b > 0 having c > 0;
select min((n as a) + (1 as b)) c from (select number n from numbers(10)) where a > 0 and b > 0 having c > 0;
