-- { echo }
select 1 == any (select number from numbers(10));
select 1 == any (select number from numbers(2, 10));

select 1 != all (select 1 from numbers(10));
select 1 != all (select number from numbers(10));

select 1 == all (select 1 from numbers(10));
select 1 == all (select number from numbers(10));

select 1 != any (select 1 from numbers(10));
select 1 != any (select number from numbers(10));

select number as a from numbers(10) where a == any (select number from numbers(3, 3));
select number as a from numbers(10) where a != any (select 5 from numbers(3, 3));

select 1 < any (select 1 from numbers(10));
select 1 <= any (select 1 from numbers(10));
select 1 < any (select number from numbers(10));
select 1 > any (select number from numbers(10));
select 1 >= any (select number from numbers(10));
select 11 > all (select number from numbers(10));
select 11 <= all (select number from numbers(11));
select 11 < all (select 11 from numbers(10));
select 11 > all (select 11 from numbers(10));
select 11 >= all (select 11 from numbers(10));
select sum(number) = any(number) from numbers(1) group by number;
select 1 == any (1);
