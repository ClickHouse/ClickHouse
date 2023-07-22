SELECT sum(length(arr)) FROM (SELECT arrayRemove(range(number % 10), x -> length(toString(x)) % 2 = 0) AS arr FROM (SELECT * FROM system.numbers LIMIT 1000));
SELECT sum(length(arr)) FROM (SELECT arrayRemove(range(number % 10), x -> length(x) % 2 = 0) AS arr FROM (SELECT * FROM system.numbers LIMIT 1000));
