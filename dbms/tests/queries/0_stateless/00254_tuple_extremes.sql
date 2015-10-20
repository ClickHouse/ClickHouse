SELECT number, (number, toDate('2015-01-01') + number) FROM system.numbers LIMIT 10 SETTINGS extremes = 1;
