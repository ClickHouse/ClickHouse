SELECT clamp(1, 10, 20);
SELECT clamp(30, 10, 20);
SELECT clamp(15, 10, 20);
SELECT clamp('a', 'b', 'c');
SELECT clamp(today(), yesterday() - 10, yesterday() + 10) - today();
SELECT clamp([], ['hello'], ['world']);
SELECT clamp(-1., -1000., 18446744073709551615.);
SELECT clamp(toNullable(123), 234, 456);
select clamp(1, null, 5);
select clamp(1, 6, null);
select clamp(1, 5, nan);
select clamp(toInt64(number), toInt64(number-1), toInt64(number+1)) from numbers(3);
select clamp(number, number-1, number+1) from numbers(3);   -- { serverError NO_COMMON_TYPE }
select clamp(1, 3, 2);   -- { serverError BAD_ARGUMENTS } 
select clamp(1, data[1], data[2])from (select arrayJoin([[1, 2], [2,3], [3,2], [4, 4]]) as data);   -- { serverError BAD_ARGUMENTS } 
