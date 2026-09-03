SELECT if(number % 2, NULL, toNullable(1)) FROM numbers(2);
SELECT if(number % 2, toNullable(1), NULL) FROM numbers(2);

