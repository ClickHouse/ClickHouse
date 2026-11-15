-- https://github.com/ClickHouse/ClickHouse/issues/77699
SELECT argMin(1 + toNullable(1), number / number) FROM numbers(1);
SELECT argMin(1 + toNullable(1), CAST('NaN', 'Float64')) FROM numbers(10000);
SELECT argMin(1 + toNullable(number), CAST('NaN', 'Float32')) FROM numbers(10000);
