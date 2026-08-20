-- Out-of-range offsets must not cause out-of-bounds pointer arithmetic in `compareSubstrings`
-- (previously flagged by UndefinedBehaviorSanitizer in `executeStringString` / `executeFixedStringString`).

-- String vs String, both offsets out of range -> equal (0).
SELECT compareSubstrings(materialize('ab'), materialize('cd'), 100, 100, 10);
-- String vs String, only the first offset out of range -> -1.
SELECT compareSubstrings(materialize('ab'), materialize('cd'), 100, 0, 10);
-- String vs String, only the second offset out of range -> 1.
SELECT compareSubstrings(materialize('ab'), materialize('cd'), 0, 100, 10);

-- FixedString vs String (the path from the fuzzer report) with a huge offset and num_bytes.
SELECT compareSubstrings(CAST(s1, 'FixedString(3)'), s2, 4, 4, 1048577)
FROM (SELECT concat('ab', toString(number % 3)) AS s1, concat('ab', toString(number % 4)) AS s2 FROM numbers(6));

-- String vs FixedString (the reverse path) with a huge offset.
SELECT compareSubstrings(s1, CAST(s2, 'FixedString(3)'), 4, 4, 1048577)
FROM (SELECT concat('ab', toString(number % 3)) AS s1, concat('ab', toString(number % 4)) AS s2 FROM numbers(6));
