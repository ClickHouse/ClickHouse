SET enable_analyzer = 1;

SELECT count((t, x_0, x_1) -> ((key_2, x_0, x_1) IN (NULL, NULL, '0.3'))) FROM numbers(10); -- { serverError UNSUPPORTED_METHOD }
SELECT count((t, x_0, x_1) -> ((key_2, x_0, x_1) IN (NULL, NULL, '0.3'))) OVER (PARTITION BY id) FROM numbers(10); -- { serverError UNSUPPORTED_METHOD }
