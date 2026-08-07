SELECT argMax(number, number + 1) FILTER(WHERE number != 99) FROM numbers(100) ;
SELECT sum(number) FILTER(WHERE number % 2 == 0) FROM numbers(100);
SELECT sumIfOrNull(number, number % 2 == 1) FILTER(WHERE 0) FROM numbers(100);
SELECT sumIfOrNull(number, number % 2 == 1) FILTER(WHERE 1) FROM numbers(100);
