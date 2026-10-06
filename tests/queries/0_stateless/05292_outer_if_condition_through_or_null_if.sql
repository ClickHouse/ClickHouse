-- The condition of an outer `-If` applies through `-OrNull` and `-OrDefault` to an inner `-If`.

SELECT sumIfOrNullIf(number, number % 2 = 0, number < 5), sumIfOrDefaultIf(number, number % 2 = 0, number < 5), countIfOrNullIf(number % 2 = 0, number < 5)
FROM numbers(10);

SELECT DISTINCT sumIfOrNullIf(number, number % 2 = 0, number < 5) OVER (), countIfOrDefaultIf(number % 2 = 0, number < 5) OVER ()
FROM numbers(10);
