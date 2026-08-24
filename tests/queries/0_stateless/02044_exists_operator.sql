-- { echo }
select exists(select 1);
select exists(select 1 except select 1);
select exists(select number from numbers(10) where number > 0);
select exists(select number from numbers(10) where number < 0);

select count() from numbers(10) where exists(select 1 except select 1);
select count() from numbers(10) where exists(select 1 intersect select 1);

select count() from numbers(10) where exists(select number from numbers(10) where number > 8);
select count() from numbers(10) where exists(select number from numbers(10) where number > 9);
