-- Regression test for STID 6488-5f6c:
-- `extractKeyValuePairs` triggered a `LOGICAL_ERROR` ("Failed to handle unexpected
-- quoting character. This is a bug") when called with `quoting_character = '\0'`
-- (NUL byte) and strategy = `accept`. Root cause: the state handler's SIMD-backed
-- `find_first_symbols_or_null` stops on NUL bytes in the haystack regardless of
-- the declared needle set, so the "unreachable" ACCEPT branch at
-- `src/Functions/keyvaluepair/impl/StateHandlerImpl.h:126` could be reached by
-- any input containing NUL bytes in the first 16 bytes of a key.
--
-- The fix rejects `'\0'` as a quoting character at configuration validation time
-- with a `BAD_ARGUMENTS` error.

-- All three strategies must reject NUL quoting character with `BAD_ARGUMENTS` (36).
SELECT extractKeyValuePairs('k:v', ':', ',', '\0', 'accept');          -- { serverError BAD_ARGUMENTS }
SELECT extractKeyValuePairs('k:v', ':', ',', '\0', 'invalid');         -- { serverError BAD_ARGUMENTS }
SELECT extractKeyValuePairs('k:v', ':', ',', '\0', 'promote');         -- { serverError BAD_ARGUMENTS }

-- Same for the escaping variant.
SELECT extractKeyValuePairsWithEscaping('k:v', ':', ',', '\0', 'accept');  -- { serverError BAD_ARGUMENTS }

-- Regression: the exact fuzzer repro (needs >= 16 bytes of data with a NUL byte
-- in the first 16 bytes so the SIMD path is taken) previously raised a
-- `LOGICAL_ERROR` exception. It must now fail cleanly with `BAD_ARGUMENTS`.
SELECT extractKeyValuePairs(toFixedString('x\0yyyyyyyyyyyyyyyy', 18), ':', ',', '\0', 'accept');  -- { serverError BAD_ARGUMENTS }

-- Non-NUL quoting characters must still work, including with NUL bytes in the data.
SELECT extractKeyValuePairs('a:1,b:2', ':', ',', '"', 'accept');
-- NUL byte in the middle of data, between two complete pairs separated by a regular
-- pair delimiter. The parser should treat the NUL as an ordinary character within
-- the surrounding value/key (here, before the 'b' start).
SELECT extractKeyValuePairs('a:1,b:2xxxxxxxxxxxxxxxx', ':', ',', '"', 'accept');
SELECT extractKeyValuePairs('"k":"v"', ':', ',', '"', 'promote');
