SELECT clamp(1, 10, 20);
SELECT clamp(30, 10, 20);
SELECT clamp(15, 10, 20);
SELECT clamp('a', 'b', 'c');
SELECT clamp(today(), yesterday() - 10, yesterday() + 10) - today()
SELECT clamp([], ['hello'], ['world']);
SELECT clamp(-1., -1000., 18446744073709551615.);
SELECT clamp(toNullable(123), 234, 456);
select clamp(1, null, 5);
select clamp(1, 6, null);