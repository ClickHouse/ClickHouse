-- Keyed SipHash over non-empty array columns with a per-row varying (non-const) key.
-- Exercises replicateForArray with distinct key values per row; existing tests only
-- use a non-const key whose value is identical in every row.

SELECT sipHash64Keyed((number, number), range(number + 1)) FROM numbers(4) ORDER BY number;
SELECT hex(sipHash128Keyed((number, number), range(number + 1))) FROM numbers(4) ORDER BY number;
SELECT hex(sipHash128ReferenceKeyed((number, number), range(number + 1))) FROM numbers(4) ORDER BY number;

-- Same, with a column that mixes empty and non-empty arrays per row.
SELECT sipHash64Keyed((number, number), if(number % 2 = 0, [], range(number))::Array(UInt64)) FROM numbers(4) ORDER BY number;
