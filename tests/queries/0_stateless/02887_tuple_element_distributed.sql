SELECT equals(tupleElement(tuple('a', 10) AS x, 1), 'a') FROM remote('127.0.0.{1,2,3}', numbers(2));
