SELECT countIf(multiIf(number < 2, NULL, if(number = 4, 1, 0))) FROM numbers(5);
