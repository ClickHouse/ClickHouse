select sumArgMin(number, number % 20), sumArgMax(number, number % 20) from numbers(100);
select sumArgMin(number, toString(number % 20)), sumArgMax(number, toString(number % 20)) from numbers(100);
select sumArgMinIf(number, number % 20, number % 2 = 0), sumArgMaxIf(number, number % 20, number % 2 = 0) from numbers(100);
