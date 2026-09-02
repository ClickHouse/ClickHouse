SELECT length(fuzzBits('stringstring', 0.5)), a FROM numbers(1) ARRAY JOIN [1, 2] AS a;
SELECT length(fuzzBits('stringstring'::FixedString(100), 0.5)), a FROM numbers(1) ARRAY JOIN [1, 2] AS a