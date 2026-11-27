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

-- FixedString vs String with an offset near the UInt64 maximum (FunctionsStress report).
SELECT compareSubstrings(materialize('abc'), materialize(toFixedString('abc', 3)), 0, 18446744073709551615, 5) FROM numbers(3);
SELECT compareSubstrings(materialize(toFixedString('abc', 3)), materialize('abc'), 18446744073709551615, 0, 5) FROM numbers(3);
SELECT compareSubstrings(materialize(toFixedString('abc', 8)), materialize('abc'), 18446744073709551611, 18446744073709551611, 5) FROM numbers(3);

-- FixedString vs String with in-range offsets over several rows.
SELECT compareSubstrings(CAST(s1, 'FixedString(3)'), s2, 1, 1, 2), compareSubstrings(s2, CAST(s1, 'FixedString(3)'), 1, 1, 2)
FROM (SELECT concat('a', toString(number % 3), 'c') AS s1, concat('b', toString(number % 4), 'c') AS s2 FROM numbers(6));
