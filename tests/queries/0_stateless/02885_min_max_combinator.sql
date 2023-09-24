select sumMin(number, number % 20), sumMax(number, number % 20) from numbers(100);
select sumMin(number, toString(number % 20)), sumMax(number, toString(number % 20)) from numbers(100);
select sumMinIf(number, number % 20, number % 2 = 0), sumMaxIf(number, number % 20, number % 2 = 0) from numbers(100);
