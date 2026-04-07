SELECT DISTINCT NULL, if(number > 0, 't', '') AS res FROM numbers(1) ORDER BY res;
