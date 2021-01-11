SELECT least('hello', 'world');
SELECT greatest('hello', 'world');
SELECT least('hello', 'world', '');
SELECT greatest('hello', 'world', 'z');

SELECT least('hello');
SELECT greatest('world');

SELECT least(1, inf, nan);
SELECT least(1, inf, nan, NULL);
SELECT greatest(1, inf, nan, NULL);
SELECT greatest(1, inf, nan);
SELECT greatest(1, inf);

SELECT least(0., -0.);
SELECT least(toNullable(123), 456);

SELECT LEAST(-1, 18446744073709551615) x, toTypeName(x);
-- This can be improved
SELECT LEAST(-1., 18446744073709551615); -- { serverError 43 }

SELECT LEAST(-1., 18446744073709551615.);
SELECT greatest(-1, 1, 4294967295);

SELECT greatest([], ['hello'], ['world']);

SELECT least([[[], []]], [[[]]], [[[]], [[]]]);
SELECT greatest([[[], []]], [[[]]], [[[]], [[]]]);

SELECT least([], [NULL]);
SELECT greatest([], [NULL]);

SELECT LEAST([NULL], [0]);
SELECT GREATEST([NULL], [0]);

SELECT Greatest();  -- { serverError 42 }
