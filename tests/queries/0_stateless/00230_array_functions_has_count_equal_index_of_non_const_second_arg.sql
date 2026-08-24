select has([0 as x], x);
select has([0 as x], materialize(x));
select has(materialize([0 as x]), x);
select has(materialize([0 as x]), materialize(x));

select has([toString(0) as x], x);
select has([toString(0) as x], materialize(x));
select has(materialize([toString(0) as x]), x);
select has(materialize([toString(0) as x]), materialize(x));

select has([toUInt64(0)], number) from system.numbers limit 10;
select has([toUInt64(0)], toUInt64(number % 3)) from system.numbers limit 10;
select has(materialize([toUInt64(0)]), number) from system.numbers limit 10;
select has(materialize([toUInt64(0)]), toUInt64(number % 3)) from system.numbers limit 10;

select has([toString(0)], toString(number)) from system.numbers limit 10;
select has([toString(0)], toString(number % 3)) from system.numbers limit 10;
select has(materialize([toString(0)]), toString(number)) from system.numbers limit 10;
select has(materialize([toString(0)]), toString(number % 3)) from system.numbers limit 10;

select 3 = countEqual([0 as x, 1, x, x], x);
select 3 = countEqual([0 as x, 1, x, x], materialize(x));
select 3 = countEqual(materialize([0 as x, 1, x, x]), x);
select 3 = countEqual(materialize([0 as x, 1, x, x]), materialize(x));

select 3 = countEqual([0 as x, 1, x, x], x) from system.numbers limit 10;
select 3 = countEqual([0 as x, 1, x, x], materialize(x)) from system.numbers limit 10;
select 3 = countEqual(materialize([0 as x, 1, x, x]), x) from system.numbers limit 10;
select 3 = countEqual(materialize([0 as x, 1, x, x]), materialize(x)) from system.numbers limit 10;

select 4 = indexOf([0, 1, 2, 3 as x], x);
select 4 = indexOf([0, 1, 2, 3 as x], materialize(x));
select 4 = indexOf(materialize([0, 1, 2, 3 as x]), x);
select 4 = indexOf(materialize([0, 1, 2, 3 as x]), materialize(x));

select 4 = indexOf([0, 1, 2, 3 as x], x) from system.numbers limit 10;
select 4 = indexOf([0, 1, 2, 3 as x], materialize(x)) from system.numbers limit 10;
select 4 = indexOf(materialize([0, 1, 2, 3 as x]), x) from system.numbers limit 10;
select 4 = indexOf(materialize([0, 1, 2, 3 as x]), materialize(x)) from system.numbers limit 10;
