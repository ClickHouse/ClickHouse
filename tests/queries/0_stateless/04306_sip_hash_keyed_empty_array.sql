-- Keyed SipHash over an all-empty nested array used to read a key for a non-existent row.
-- The key here is non-const (it varies per row), which is what triggered the out-of-bounds read.

SELECT sipHash64Keyed((number, number), []::Array(UInt8)) FROM numbers(2);
SELECT hex(sipHash128Keyed((number, number), []::Array(UInt8))) FROM numbers(2);
SELECT hex(sipHash128ReferenceKeyed((number, number), []::Array(UInt8))) FROM numbers(2);

-- Deeper nesting: an empty array as the value side of a Map, as in the original report.
SELECT sipHash64Keyed((number, number), map(toDate('2149-05-19'), []::Array(UInt8))) FROM numbers(2);
SELECT hex(sipHash128Keyed((number, number), [[]]::Array(Array(UInt8)))) FROM numbers(2);
SELECT hex(sipHash128ReferenceKeyed((number, number), []::Array(Array(UInt8)))) FROM numbers(2);
