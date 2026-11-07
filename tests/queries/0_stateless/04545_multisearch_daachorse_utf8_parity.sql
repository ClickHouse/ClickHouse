-- Tags: no-fasttest

-- The daachorse (Aho-Corasick) path, taken for >255 needles or force_daachorse_for_multi_search=1,
-- must match the legacy searcher on valid UTF-8. Each parity assertion below compares the forced path
-- against the legacy path and prints 1 when they agree.
-- On malformed UTF-8 the legacy searcher has no single rule: whether a match can start inside an
-- invalid sequence depends on the needle length and on the haystack length. The daachorse path folds
-- each byte that does not start a valid sequence into one byte that no needle contains. A match never
-- covers such a byte, and the next byte can start a match. The last query pins these results.
-- Each haystack is wrapped in `materialize`: when all arguments are constant, the needles reach the
-- function as a non-constant column, and the daachorse path does not run.

SET send_logs_level = 'fatal';

-- Lowercase mapping whose UTF-8 encoding grows (U+023A Ⱥ, 2 bytes -> U+2C65 ⱥ, 3 bytes).
SELECT (SELECT multiSearchAnyCaseInsensitiveUTF8(materialize('Ⱥ'), ['ⱥ']) SETTINGS force_daachorse_for_multi_search = 1)
     = (SELECT multiSearchAnyCaseInsensitiveUTF8(materialize('Ⱥ'), ['ⱥ']) SETTINGS force_daachorse_for_multi_search = 0);
SELECT (SELECT multiSearchAnyCaseInsensitiveUTF8(materialize('a Ⱥ b'), ['ⱥ']) SETTINGS force_daachorse_for_multi_search = 1)
     = (SELECT multiSearchAnyCaseInsensitiveUTF8(materialize('a Ⱥ b'), ['ⱥ']) SETTINGS force_daachorse_for_multi_search = 0);

-- KELVIN SIGN (U+212A) matches only itself, although `Poco::Unicode::toLower` maps it to 'k'.
SELECT (SELECT multiSearchAnyCaseInsensitiveUTF8(materialize('K'), ['k']) SETTINGS force_daachorse_for_multi_search = 1)
     = (SELECT multiSearchAnyCaseInsensitiveUTF8(materialize('K'), ['k']) SETTINGS force_daachorse_for_multi_search = 0);
SELECT (SELECT multiSearchAnyCaseInsensitiveUTF8(materialize('k'), ['K']) SETTINGS force_daachorse_for_multi_search = 1)
     = (SELECT multiSearchAnyCaseInsensitiveUTF8(materialize('k'), ['K']) SETTINGS force_daachorse_for_multi_search = 0);

SELECT
    multiSearchAnyCaseInsensitiveUTF8(materialize('Ⱥ'), ['ⱥ']),
    multiSearchAnyCaseInsensitiveUTF8(materialize('K'), ['k']),
    -- A truncated sequence before ASCII: the 'A' matches.
    multiSearchAnyCaseInsensitiveUTF8(materialize(unhex('E441')), ['a']),
    -- A truncated sequence before a 4-byte sequence: the emoji matches.
    multiSearchAnyCaseInsensitiveUTF8(materialize(concat('x', unhex('E4'), '😀', 'y')), ['😀']),
    multiSearchAnyCaseInsensitiveUTF8(materialize(concat(unhex('F09F98'), '😀')), ['😀']),
    -- An invalid byte never matches, and it never joins its neighbours into a match.
    multiSearchAnyCaseInsensitiveUTF8(materialize(unhex('E4')), [unhex('00')]),
    multiSearchAnyCaseInsensitiveUTF8(materialize(concat('x', unhex('E4'), 'a')), ['xa'])
SETTINGS force_daachorse_for_multi_search = 1;
