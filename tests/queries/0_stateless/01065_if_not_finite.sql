SELECT ifNotFinite(round(1 / number, 2), 111) FROM numbers(10);

SELECT ifNotFinite(1, 2);
SELECT ifNotFinite(-1.0, 2);
SELECT ifNotFinite(nan, 2);
SELECT ifNotFinite(-1 / 0, 2);
SELECT ifNotFinite(log(0), NULL);
SELECT ifNotFinite(sqrt(-1), -42);
SELECT ifNotFinite(1234567890123456789, -1234567890123456789); -- { serverError 386 }

SELECT ifNotFinite(NULL, 1);
