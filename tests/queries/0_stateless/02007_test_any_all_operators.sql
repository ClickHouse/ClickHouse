-- { echo }
select 1 == any (select number from numbers(10));
select 1 == any (select number from numbers(2, 10));
select 1 == all (select 1 from numbers(10));
select 1 == all (select number from numbers(10));
select number as a from numbers(10) where a == any (select number from numbers(3, 3));

-- TODO: Incorrect:
select 1 != any (select 1 from numbers(10));
select 1 != all (select 1 from numbers(10));
select number as a from numbers(10) where a != any (select number from numbers(3, 3));

