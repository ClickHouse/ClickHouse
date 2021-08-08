-- { echo }
select 1 intersect select 1;
select 2 intersect select 1;
select 1 except select 1;
select 2 except select 1;
select number from numbers(5, 5) intersect select number from numbers(20);
select number from numbers(10) except select number from numbers(5);
select number, number+10 from numbers(12) except select number+5, number+15 from numbers(10);
