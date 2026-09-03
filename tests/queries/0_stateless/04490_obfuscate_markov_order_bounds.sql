-- The `obfuscate` table function builds a Markov model that preallocates a vector of
-- `obfuscate_markov_order` code points and does work proportional to the order for every code point.
-- A huge order used to throw an internal `std::length_error` (reported as a logical error) from the
-- vector allocation; it is now rejected up front with a clear `BAD_ARGUMENTS` exception.
-- LIMIT must be pushed inside because obfuscate(...) is an infinite source.

-- The exact value found by the AST fuzzer.
SELECT * FROM obfuscate(SELECT toString(number) AS s FROM numbers(8)) LIMIT 1 SETTINGS obfuscate_markov_order = 9223372036854775806; -- { serverError BAD_ARGUMENTS }

-- The first value above the allowed maximum is rejected.
SELECT * FROM obfuscate(SELECT toString(number) AS s FROM numbers(8)) LIMIT 1 SETTINGS obfuscate_markov_order = 1001; -- { serverError BAD_ARGUMENTS }

-- The boundary values of the allowed range are accepted.
SELECT count() FROM (SELECT * FROM obfuscate(SELECT toString(number) AS s FROM numbers(8)) LIMIT 8 SETTINGS obfuscate_markov_order = 1, obfuscate_seed = 'seed');
SELECT count() FROM (SELECT * FROM obfuscate(SELECT toString(number) AS s FROM numbers(8)) LIMIT 8 SETTINGS obfuscate_markov_order = 1000, obfuscate_seed = 'seed');
