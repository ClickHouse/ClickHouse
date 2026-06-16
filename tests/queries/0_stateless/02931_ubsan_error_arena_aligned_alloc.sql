-- previously it caused `runtime error: applying non-zero offset 7 to null pointer`
SELECT sumResample(65535, 20, 1)(number, number % 20) FROM numbers(200);
