SELECT byteSize(123, 456.7) AS x, isConstant(x);
SELECT byteSize(number, number + 1) AS x, isConstant(x) FROM numbers(2);
SELECT byteSize();
