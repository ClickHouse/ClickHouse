-- Reproducer for the bug where sipHash128Keyed with a Map argument containing
-- nested empty arrays caused an assertion failure in getKey due to incorrect
-- offset chaining through multiple array nesting levels.
SELECT hex(sipHash128Keyed((CAST('1', 'UInt64'), CAST(materialize('2'), 'UInt64')), map([0], 1, [], 3))) FROM numbers(2);
