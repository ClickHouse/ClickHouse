-- The integer obfuscation models permute values inside their log2 bucket using 64-bit arithmetic
-- (`getUInt`/`getInt` and a `UInt64`/`Int64` `Field`), so they support only native integer widths.
-- Wide integers (`Int128`/`UInt128`/`Int256`/`UInt256`) are rejected up front with a clean
-- `NOT_IMPLEMENTED` exception instead of failing with an internal `BAD_GET` error during generation.

SELECT * FROM obfuscate(SELECT toUInt128(1) AS x) SETTINGS obfuscate_seed = 'seed'; -- { serverError NOT_IMPLEMENTED }
SELECT * FROM obfuscate(SELECT toInt128(1) AS x) SETTINGS obfuscate_seed = 'seed'; -- { serverError NOT_IMPLEMENTED }
SELECT * FROM obfuscate(SELECT toUInt256(1) AS x) SETTINGS obfuscate_seed = 'seed'; -- { serverError NOT_IMPLEMENTED }
SELECT * FROM obfuscate(SELECT toInt256(1) AS x) SETTINGS obfuscate_seed = 'seed'; -- { serverError NOT_IMPLEMENTED }

-- The minimum value of each native signed type is the only value in its log2 bucket whose magnitude is
-- not representable as a positive value of the same width, so it is kept as is to preserve the sign.
-- Without the per-width handling, `Int8` `-128` could be permuted to a magnitude in 128..255 and narrow
-- back into a positive value.
SELECT DISTINCT x FROM (SELECT * FROM obfuscate(SELECT toInt8(-128) AS x FROM numbers(10)) LIMIT 10) SETTINGS obfuscate_seed = 'seed';
SELECT DISTINCT x FROM (SELECT * FROM obfuscate(SELECT toInt16(-32768) AS x FROM numbers(10)) LIMIT 10) SETTINGS obfuscate_seed = 'seed';
SELECT DISTINCT x FROM (SELECT * FROM obfuscate(SELECT toInt32(-2147483648) AS x FROM numbers(10)) LIMIT 10) SETTINGS obfuscate_seed = 'seed';

-- Other narrow negative values stay negative.
SELECT max(x) < 0 FROM (SELECT * FROM obfuscate(SELECT toInt8(-100) AS x FROM numbers(10)) LIMIT 100) SETTINGS obfuscate_seed = 'seed';
