set enable_analyzer=1;

SELECT ((number % 2) = 0) = true AS isEven FROM remote('localhos{t,t,t}', numbers(10)) GROUP BY all ORDER BY all;
