SELECT transform(number / 2, [0.5, 1.5], ['Hello', 'World'], '-') FROM numbers(10);
SELECT transform(number / 2, [1.0, 2.0], ['Hello', 'World'], '-') FROM numbers(10);
SELECT transform(number / 2, [1, 2], ['Hello', 'World'], '-') FROM numbers(10);
