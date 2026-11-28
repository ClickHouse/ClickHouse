-- A nondeterministic function must produce an independent value per row even when all of its
-- arguments are constants.

SELECT uniqExact(fuzzBits('aaaaaaaaaaaaaaaa', 0.4)) > 1 FROM numbers(100);
SELECT uniqExact(fuzzBits(toFixedString('aaaaaaaaaaaaaaaa', 16), 0.4)) > 1 FROM numbers(100);

-- The shape and the length of the result are unchanged.
SELECT length(fuzzBits('aaaaaaaaaaaaaaaa', 0.4)) FROM numbers(3);
SELECT fuzzBits('aaaa', 0.) FROM numbers(2);
SELECT count() FROM (SELECT fuzzBits(materialize('aaaaaaaaaaaaaaaa'), 0.4) FROM numbers(10));
SELECT fuzzBits('aaaa', 0.4) FROM numbers(0);
