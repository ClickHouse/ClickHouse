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
select sum(number) = max(number) from numbers(1) group by number;
select 1 == any (select 1);

-- Behavior change for ambiguous shapes that used to parse `any(...)` / `all(...)` as a function call:
-- the non-subquery RHS now goes through the PostgreSQL `expr OP ANY/ALL(arr)` rewrite and yields a
-- type error because the rewritten `has` / `arrayExists` / `arrayAll` expects an array. These two
-- queries used to succeed; preserved here as documentation of the regression.
select sum(number) = any(number) from numbers(1) group by number; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
select 1 == any (1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
