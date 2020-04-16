-- for small cardinality the 64 bit hash perform worse, but for 1e10:
-- 4 byte hash: 2.8832809652e10
-- 8 byte hash: 0.9998568925e10
-- but hence checking with 1e10 values takes too much time (~45 secs), this
-- test is just to ensure that the result is different (and to document the
-- outcome).

SELECT uniqCombined(number)   FROM numbers(toUInt64(1e7));
SELECT uniqCombined64(number) FROM numbers(toUInt64(1e7));
