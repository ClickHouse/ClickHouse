set enable_dynamic_type=1;

select coalesce(number % 2 ? NULL : number::Dynamic, 42) as res from numbers(5);
select coalesce(number % 2 ? NULL : number::Dynamic, number % 3 ? NULL : 42) as res from numbers(5);
select coalesce(number % 2 ? NULL : number, number % 3 ? NULL : 42::Dynamic) as res from numbers(5);
select coalesce(number % 2 ? NULL : number::Dynamic, number % 3 ? NULL : 42::Dynamic) as res from numbers(5);
select coalesce(number % 2 ? NULL : number::Dynamic, number % 3 ? NULL : 42, number % 4 == 1 ? NULL : 43) as res from numbers(10);
select coalesce(number % 2 ? NULL : number, number % 3 ? NULL : 42::Dynamic, number % 4 == 1 ? NULL : 43) as res from numbers(10);
select coalesce(number % 2 ? NULL : number, number % 3 ? NULL : 42, number % 4 == 1 ? NULL : 43::Dynamic) as res from numbers(10);
select coalesce(number % 2 ? NULL : number, number % 3 ? NULL : 42, number % 4 == 1 ? NULL : 43::Dynamic) as res from numbers(10);
select coalesce(number % 2 ? NULL : number::Dynamic, number % 3 ? NULL : 42::Dynamic, number % 4 == 1 ? NULL : 43) as res from numbers(10);
select coalesce(number % 2 ? NULL : number, number % 3 ? NULL : 42::Dynamic, number % 4 == 1 ? NULL : 43::Dynamic) as res from numbers(10);
select coalesce(number % 2 ? NULL : number::Dynamic, number % 3 ? NULL : 42, number % 4 == 1 ? NULL : 43::Dynamic) as res from numbers(10);
select coalesce(number % 2 ? NULL : number::Dynamic, number % 3 ? NULL : 42::Dynamic, number % 4 == 1 ? NULL : 43::Dynamic) as res from numbers(10);

