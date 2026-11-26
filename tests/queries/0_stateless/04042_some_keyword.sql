-- Test that SOME keyword is valid and behaves exactly like ANY keyword in subquery expressions
-- { echo }
select 1 == some (select number from numbers(10));
select 1 == some (select number from numbers(2, 10));

select 1 != some (select 1 from numbers(10));
select 1 != some (select number from numbers(10));

select number as a from numbers(10) where a == some (select number from numbers(3, 3));
select number as a from numbers(10) where a != some (select 5 from numbers(3, 3));

select 1 < some (select 1 from numbers(10));
select 1 <= some (select 1 from numbers(10));
select 1 < some (select number from numbers(10));
select 1 > some (select number from numbers(10));
select 1 >= some (select number from numbers(10));
