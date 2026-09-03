SELECT transform(number, [], [1]) FROM numbers(10);
SELECT transform(number, [], [], 'Hello') FROM numbers(10);
SELECT transform(number, [], [], 'Hello ' || number::String) FROM numbers(10);
