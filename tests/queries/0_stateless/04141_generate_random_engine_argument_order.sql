-- Regression test for `GenerateRandom` engine argument order:
-- the second argument must be used as `max_string_length`, while previously `seed` was used.

DROP TABLE IF EXISTS generate_random_engine_argument_order;

CREATE TABLE generate_random_engine_argument_order
(
    s String,
    a Array(UInt8)
)
ENGINE = GenerateRandom(1024, 16, 10);

SELECT max(length(s)) <= 16, max(length(a)) <= 10
FROM
(
    SELECT s, a
    FROM generate_random_engine_argument_order
    LIMIT 1000
)
SETTINGS max_threads = 1;

DROP TABLE generate_random_engine_argument_order;
