set enable_dynamic_type = 1;

select ifNull(number % 2 ? NULL : number::Dynamic, 42) from numbers(5);

