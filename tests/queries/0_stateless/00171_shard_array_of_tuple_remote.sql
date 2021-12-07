SELECT arrayMap((x, y) -> (x, y), [1, 2, 3], [4, 5, 6]) FROM remote('127.0.0.{2,3}', system.one) ORDER BY rand();
