SELECT * FROM (SELECT nan, number FROM system.numbers) WHERE number % 100 = 1 LIMIT 1;
