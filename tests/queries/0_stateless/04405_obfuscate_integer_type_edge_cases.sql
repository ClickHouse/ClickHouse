-- The minimum value of each native signed type is the only value in its log2 bucket whose magnitude is
-- not representable as a positive value of the same width, so it is kept as is to preserve the sign.
-- Without the per-width handling, `Int8` `-128` could be permuted to a magnitude in 128..255 and narrow
-- back into a positive value.
SELECT DISTINCT x FROM (SELECT * FROM obfuscate(SELECT toInt8(-128) AS x FROM numbers(10)) LIMIT 10) SETTINGS obfuscate_seed = 'seed';
SELECT DISTINCT x FROM (SELECT * FROM obfuscate(SELECT toInt16(-32768) AS x FROM numbers(10)) LIMIT 10) SETTINGS obfuscate_seed = 'seed';
SELECT DISTINCT x FROM (SELECT * FROM obfuscate(SELECT toInt32(-2147483648) AS x FROM numbers(10)) LIMIT 10) SETTINGS obfuscate_seed = 'seed';

-- Other narrow negative values stay negative.
SELECT max(x) < 0 FROM (SELECT * FROM obfuscate(SELECT toInt8(-100) AS x FROM numbers(10)) LIMIT 100) SETTINGS obfuscate_seed = 'seed';
