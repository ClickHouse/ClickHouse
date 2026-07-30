-- Tags: no-fasttest

SET send_logs_level = 'fatal';

SELECT
    multiSearchAny('abc', ['']),
    multiSearchAny('abc', ['x', '', 'z']),
    multiSearchAnyCaseInsensitive('abc', ['']),
    multiSearchAny('abc', ['a', 'a', 'b']),
    multiSearchAny('xyz', ['a', 'a', 'b']),
    multiSearchAnyCaseInsensitive('abc', ['ABC', 'abc']),
    multiSearchAnyCaseInsensitive('über', ['ÜBER']),
    multiSearchAnyCaseInsensitive('HELLO world', ['hello']),
    multiSearchAnyCaseInsensitiveUTF8('über', ['ÜBER']),
    multiSearchAnyCaseInsensitiveUTF8('plain i', ['İ']),
    multiSearchAnyCaseInsensitiveUTF8('ǆ', ['ǅ']),
    multiSearchAnyCaseInsensitiveUTF8('ⅰ', ['Ⅰ']),
    multiSearchAnyCaseInsensitiveUTF8(unhex('FF41'), [unhex('FF61')])
SETTINGS force_daachorse_for_multi_search = 1;

WITH arrayMap(i -> concat('needle_', toString(i)), range(255)) AS needles
SELECT
    length(needles),
    multiSearchAny('needle_254', needles),
    multiSearchAny('missing', needles);

WITH arrayMap(i -> concat('needle_', toString(i)), range(256)) AS needles
SELECT
    length(needles),
    multiSearchAny('needle_255', needles),
    multiSearchAnyCaseInsensitive('NEEDLE_255', needles),
    multiSearchAnyUTF8('needle_255', needles),
    multiSearchAnyCaseInsensitiveUTF8('NEEDLE_255', needles),
    multiSearchAny('missing', needles);
