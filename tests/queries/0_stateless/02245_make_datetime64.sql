select makeDateTime64(1991, 8, 24, 21, 4, 0);
select makeDateTime64(1991, 8, 24, 21, 4, 0, 123);
select makeDateTime64(1991, 8, 24, 21, 4, 0, 1234, 6);
select makeDateTime64(1991, 8, 24, 21, 4, 0, 1234, 7, 'CET');
select cast(makeDateTime64(1991, 8, 24, 21, 4, 0, 1234, 7, 'CET') as DateTime64(7, 'UTC'));

select toTypeName(makeDateTime64(1991, 8, 24, 21, 4, 0));
select toTypeName(makeDateTime64(1991, 8, 24, 21, 4, 0, 123));
select toTypeName(makeDateTime64(1991, 8, 24, 21, 4, 0, 1234, 6));
select toTypeName(makeDateTime64(1991, 8, 24, 21, 4, 0, 1234, 7, 'CET'));
select toTypeName(cast(makeDateTime64(1991, 8, 24, 21, 4, 0, 1234, 7, 'CET') as DateTime64(7, 'UTC')));

select makeDateTime64(1925, 1, 1, 0, 0, 0, 0, 9, 'UTC');
select makeDateTime64(1924, 12, 31, 23, 59, 59, 999999999, 9, 'UTC');
select makeDateTime64(2283, 11, 11, 23, 59, 59, 99999999, 8, 'UTC');
select makeDateTime64(2283, 11, 11, 23, 59, 59, 999999999, 9, 'UTC'); -- { serverError 407 }
select makeDateTime64(2262, 4, 11, 23, 47, 16, 854775807, 9, 'UTC');
select makeDateTime64(2262, 4, 11, 23, 47, 16, 854775808, 9, 'UTC'); -- { serverError 407 }
select makeDateTime64(2262, 4, 11, 23, 47, 16, 85477581, 8, 'UTC');

select makeDateTime64(1991, 8, 24, 21, 4, 0, 1234, 0, 'CET');
select makeDateTime64(1991, 8, 24, 21, 4, 0, 1234, 1, 'CET');
select makeDateTime64(1991, 8, 24, 21, 4, 0, 1234, 2, 'CET');
select makeDateTime64(1991, 8, 24, 21, 4, 0, 1234, 3, 'CET');
select makeDateTime64(1991, 8, 24, 21, 4, 0, 1234, 4, 'CET');
select makeDateTime64(1991, 8, 24, 21, 4, 0, 1234, 5, 'CET');
select makeDateTime64(1991, 8, 24, 21, 4, 0, 1234, 6, 'CET');
select makeDateTime64(1991, 8, 24, 21, 4, 0, 1234, 7, 'CET');
select makeDateTime64(1991, 8, 24, 21, 4, 0, 1234, 8, 'CET');
select makeDateTime64(1991, 8, 24, 21, 4, 0, 1234, 9, 'CET');
select makeDateTime64(1991, 8, 24, 21, 4, 0, 1234, 10, 'CET'); -- { serverError 69 }
select makeDateTime64(1991, 8, 24, 21, 4, 0, 1234, -1, 'CET'); -- { serverError 69 }

select makeDateTime64(1984, 0, 1, 0, 0, 0, 0, 9, 'UTC');
select makeDateTime64(1984, 1, 0, 0, 0, 0, 0, 9, 'UTC');
select makeDateTime64(1984, 13, 1, 0, 0, 0, 0, 9, 'UTC');
select makeDateTime64(1984, 1, 41, 0, 0, 0, 0, 9, 'UTC');
select makeDateTime64(1984, 1, 1, 25, 0, 0, 0, 9, 'UTC');
select makeDateTime64(1984, 1, 1, 0, 70, 0, 0, 9, 'UTC');
select makeDateTime64(1984, 1, 1, 0, 0, 70, 0, 9, 'UTC');
select makeDateTime64(1984, 1, 1, 0, 0, 0, 0, 9, 'not a timezone'); -- { serverError 1000 }

select makeDateTime64(1984, 1, 1, 2, 3, 4, 5, 9, 'UTC');
select makeDateTime64(1984, 2, 29, 2, 3, 4, 5, 9, 'UTC');
select makeDateTime64(1983, 2, 29, 2, 3, 4, 5, 9, 'UTC');
select makeDateTime64(1984, 2, 30, 2, 3, 4, 5, 9, 'UTC');
select makeDateTime64(1983, 2, 30, 2, 3, 4, 5, 9, 'UTC');
select makeDateTime64(1984, 2, 31, 2, 3, 4, 5, 9, 'UTC');
select makeDateTime64(1983, 2, 31, 2, 3, 4, 5, 9, 'UTC');
select makeDateTime64(1984, 2, 32, 2, 3, 4, 5, 9, 'UTC');
select makeDateTime64(1983, 2, 32, 2, 3, 4, 5, 9, 'UTC');

select makeDateTime64(-1984, 1, 1, 2, 3, 4, 5, 9, 'UTC');
select makeDateTime64(1984, -1, 1, 2, 3, 4, 5, 9, 'UTC');
select makeDateTime64(1984, 1, -1, 2, 3, 4, 5, 9, 'UTC');
select makeDateTime64(1984, 1, 1, -1, 3, 4, 5, 9, 'UTC');
select makeDateTime64(1984, 1, 1, 2, -1, 4, 5, 9, 'UTC');
select makeDateTime64(1984, 1, 1, 2, 3, -1, 5, 9, 'UTC');
select makeDateTime64(1984, 1, 1, 2, 3, 4, -1, 9, 'UTC');

select makeDateTime64(NaN, 1, 1, 2, 3, 4, 5, 9, 'UTC');
select makeDateTime64(1984, NaN, 1, 2, 3, 4, 5, 9, 'UTC');
select makeDateTime64(1984, 1, NaN, 2, 3, 4, 5, 9, 'UTC');
select makeDateTime64(1984, 1, 1, NaN, 3, 4, 5, 9, 'UTC');
select makeDateTime64(1984, 1, 1, 2, NaN, 4, 5, 9, 'UTC');
select makeDateTime64(1984, 1, 1, 2, 3, NaN, 5, 9, 'UTC');
select makeDateTime64(1984, 1, 1, 2, 3, 4, NaN, 9, 'UTC');

select makeDateTime64(1984.5, 1, 1, 0, 0, 0, 0, 9, 'UTC');
select makeDateTime64(1984, 1.5, 1, 0, 0, 0, 0, 9, 'UTC');
select makeDateTime64(1984, 1, 1.5, 0, 0, 0, 0, 9, 'UTC');
select makeDateTime64(1984, 1, 1, 0.5, 0, 0, 0, 9, 'UTC');
select makeDateTime64(1984, 1, 1, 0, 0.5, 0, 0, 9, 'UTC');
select makeDateTime64(1984, 1, 1, 0, 0, 0.5, 0, 9, 'UTC');
select makeDateTime64(1984, 1, 1, 0, 0, 0, 0.5, 9, 'UTC');
select makeDateTime64(1984, 1, 1, 0, 0, 0, 0, 9.5, 'UTC');

select makeDateTime64(65537, 8, 24, 21, 4, 0);
select makeDateTime64(1991, 65537, 24, 21, 4, 0);
select makeDateTime64(1991, 8, 65537, 21, 4, 0);
select makeDateTime64(1991, 8, 24, 65537, 4, 0);
select makeDateTime64(1991, 8, 24, 21, 65537, 0);
select makeDateTime64(1991, 8, 24, 21, 4, 65537);

select makeDateTime64(year, 1, 1, 1, 0, 0, 0, precision, timezone) from (
    select 1984 as year, 5 as precision, 'UTC' as timezone
    union all
    select 1985 as year, 5 as precision, 'UTC' as timezone
); -- { serverError 43 }
