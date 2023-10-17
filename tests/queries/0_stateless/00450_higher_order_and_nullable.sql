SELECT arrayMap(x -> x % 2 = 0 ? NULL : x, range(number)) FROM system.numbers LIMIT 10;
