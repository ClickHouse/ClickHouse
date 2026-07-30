-- Tags: no-fasttest

-- The daachorse (Aho-Corasick) path, taken for >255 needles or force_daachorse_for_multi_search=1,
-- must match the legacy searcher on well-formed / UTF-8 case-fold inputs. Each assertion below
-- compares the forced path against the legacy path and prints 1 when they agree. The cases exercise
-- malformed UTF-8 and length-growing lowercase mappings, where a naive fold diverges from the legacy
-- searcher (which advances over invalid input by UTF8::seqLength). Binary-data parity is not claimed:
-- the legacy SIMD searcher has a pre-existing bug on some non-ASCII last-byte cases.

SET send_logs_level = 'fatal';

-- Malformed lead byte followed by ASCII: legacy skips the whole nominal 3-byte sequence, so the 'A'
-- never matches 'a'. A per-byte fold would keep the 'A' and spuriously match.
SELECT (SELECT multiSearchAnyCaseInsensitiveUTF8(unhex('E441'), ['a']) SETTINGS force_daachorse_for_multi_search = 1)
     = (SELECT multiSearchAnyCaseInsensitiveUTF8(unhex('E441'), ['a']) SETTINGS force_daachorse_for_multi_search = 0);

-- Truncated lead byte against a NUL needle: both paths must agree on the malformed sequence handling.
SELECT (SELECT multiSearchAnyCaseInsensitiveUTF8(unhex('E4'), [unhex('00')]) SETTINGS force_daachorse_for_multi_search = 1)
     = (SELECT multiSearchAnyCaseInsensitiveUTF8(unhex('E4'), [unhex('00')]) SETTINGS force_daachorse_for_multi_search = 0);

-- Lowercase mapping whose UTF-8 encoding grows (U+023A Ⱥ, 2 bytes -> U+2C65 ⱥ, 3 bytes).
SELECT (SELECT multiSearchAnyCaseInsensitiveUTF8('Ⱥ', ['ⱥ']) SETTINGS force_daachorse_for_multi_search = 1)
     = (SELECT multiSearchAnyCaseInsensitiveUTF8('Ⱥ', ['ⱥ']) SETTINGS force_daachorse_for_multi_search = 0);
SELECT (SELECT multiSearchAnyCaseInsensitiveUTF8('a Ⱥ b', ['ⱥ']) SETTINGS force_daachorse_for_multi_search = 1)
     = (SELECT multiSearchAnyCaseInsensitiveUTF8('a Ⱥ b', ['ⱥ']) SETTINGS force_daachorse_for_multi_search = 0);

-- Malformed bytes embedded between valid content must not glue neighbours into a false match.
SELECT (SELECT multiSearchAnyCaseInsensitiveUTF8(concat('x', unhex('E4'), 'a'), ['xa']) SETTINGS force_daachorse_for_multi_search = 1)
     = (SELECT multiSearchAnyCaseInsensitiveUTF8(concat('x', unhex('E4'), 'a'), ['xa']) SETTINGS force_daachorse_for_multi_search = 0);

-- Pinned expected values (legacy path): malformed sequences do not match, valid growing map does.
SELECT
    multiSearchAnyCaseInsensitiveUTF8(unhex('E441'), ['a']),
    multiSearchAnyCaseInsensitiveUTF8(unhex('E4'), [unhex('00')]),
    multiSearchAnyCaseInsensitiveUTF8('Ⱥ', ['ⱥ']),
    multiSearchAnyCaseInsensitiveUTF8('a Ⱥ b', ['ⱥ']);
