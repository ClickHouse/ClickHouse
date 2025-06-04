set enable_analyzer=1;

-- { echo On }

select * from numbers(1) l inner join system.one r using (number as dummy);
select * from system.one l inner join numbers(1) r using (dummy as number);

select * from numbers(2) l inner join (select number + 1 as dummy from numbers(1)) r using (number as dummy);
select * from (select number + 1 as dummy from numbers(1)) l inner join numbers(2) r using (dummy as number);
