-- `transformSigned` keeps the sign of the input. The minimum signed number is the only value
-- whose magnitude is not representable as a positive Int64, so it is kept as is.

SELECT DISTINCT x FROM (SELECT * FROM obfuscate(SELECT toInt64(-9223372036854775808) AS x FROM numbers(10)) LIMIT 10) SETTINGS obfuscate_seed = 'seed';

-- Other negative values stay negative.
SELECT max(x) < 0 FROM (SELECT * FROM obfuscate(SELECT toInt64(-100 - number * 1000) AS x FROM numbers(10)) LIMIT 100) SETTINGS obfuscate_seed = 'seed';
