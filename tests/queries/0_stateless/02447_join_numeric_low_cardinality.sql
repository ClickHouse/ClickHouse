-- https://github.com/ClickHouse/ClickHouse/issues/57243

SET enable_analyzer = 1;
SET allow_suspicious_low_cardinality_types = 1;

DROP TABLE IF EXISTS arrays_test;

CREATE TABLE arrays_test
(
    `s` String,
    `arr1` Array(LowCardinality(UInt8))
)
ENGINE = Log();

INSERT INTO arrays_test
VALUES ('Hello', [1,2]), ('World', [3,4,5]), ('Goodbye', []);

-- This always worked because 10 < 256
SELECT s, range(0, 10)::Array(LowCardinality(UInt64)) as arr1
FROM arrays_test
ARRAY JOIN arr1
settings enable_unaligned_array_join = 1;

-- This failed because 300 > 256 -> underlying type can't be integer
SELECT s, range(0, 300)::Array(LowCardinality(UInt64)) as arr1
FROM arrays_test
ARRAY JOIN arr1
settings enable_unaligned_array_join = 1;

-- Lets try something different
DROP TABLE arrays_test;

-- Create a similar table but with Array(LowCardinality(UInt16))
CREATE TABLE arrays_test
(
    `s` String,
    `arr1` Array(LowCardinality(UInt16))
)
ENGINE = Log();

INSERT INTO arrays_test
VALUES ('Hello', [1,2]), ('World', [3,4,5]), ('Goodbye', []);

-- This also failed (The cardinality size for 257 elements > UInt8 max 256)
SELECT s, range(0, 257)::Array(LowCardinality(UInt16)) as arr1
FROM arrays_test
ARRAY JOIN arr1
settings enable_unaligned_array_join = 1;

-- { echoOff }

DROP TABLE arrays_test;
