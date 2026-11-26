-- The `null_ratio` setting of `generateRandom` and of `ENGINE = GenerateRandom`.
-- A measured share uses 100000 rows, which put its 6-sigma interval well inside the asserted
-- bounds; the exact counts of the ratios 0 and 1 need no more than 10000.

-- Table function.
SELECT countIf(x IS NULL) / count() BETWEEN 0.49 AND 0.51
FROM (SELECT * FROM generateRandom('x Nullable(UInt8)', 1, SETTINGS null_ratio = 0.5) LIMIT 100000);

SELECT countIf(x IS NULL)
FROM (SELECT * FROM generateRandom('x Nullable(UInt8)', 1, SETTINGS null_ratio = 0) LIMIT 10000);

SELECT countIf(x IS NULL)
FROM (SELECT * FROM generateRandom('x Nullable(UInt8)', 1, SETTINGS null_ratio = 1) LIMIT 10000);

-- The default ratio is 1 in 16.
SELECT countIf(x IS NULL) / count() BETWEEN 0.055 AND 0.07
FROM (SELECT * FROM generateRandom('x Nullable(UInt8)', 1) LIMIT 100000);

-- The same three ratios on an engine table.
DROP TABLE IF EXISTS null_ratio_half;
CREATE TABLE null_ratio_half (x Nullable(UInt8)) ENGINE = GenerateRandom(1) SETTINGS null_ratio = 0.5;
SELECT countIf(x IS NULL) / count() BETWEEN 0.49 AND 0.51 FROM (SELECT * FROM null_ratio_half LIMIT 100000);

DROP TABLE IF EXISTS null_ratio_zero;
CREATE TABLE null_ratio_zero (x Nullable(UInt8)) ENGINE = GenerateRandom(1) SETTINGS null_ratio = 0;
SELECT countIf(x IS NULL) FROM (SELECT * FROM null_ratio_zero LIMIT 10000);

DROP TABLE IF EXISTS null_ratio_one;
CREATE TABLE null_ratio_one (x Nullable(UInt8)) ENGINE = GenerateRandom(1) SETTINGS null_ratio = 1;
SELECT countIf(x IS NULL) FROM (SELECT * FROM null_ratio_one LIMIT 10000);

DROP TABLE IF EXISTS null_ratio_default;
CREATE TABLE null_ratio_default (x Nullable(UInt8)) ENGINE = GenerateRandom(1);
SELECT countIf(x IS NULL) / count() BETWEEN 0.055 AND 0.07 FROM (SELECT * FROM null_ratio_default LIMIT 100000);

DROP TABLE null_ratio_half;
DROP TABLE null_ratio_zero;
DROP TABLE null_ratio_one;
DROP TABLE null_ratio_default;
