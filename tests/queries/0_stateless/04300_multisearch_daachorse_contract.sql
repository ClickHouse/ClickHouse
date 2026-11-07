-- Tags: no-fasttest

SET send_logs_level = 'fatal';

-- Each haystack is wrapped in `materialize`: when all arguments are constant, the needles reach the
-- function as a non-constant column, and the daachorse path does not run.
SELECT
    multiSearchAny(materialize('abc'), ['']),
    multiSearchAny(materialize('abc'), ['x', '', 'z']),
    multiSearchAnyCaseInsensitive(materialize('abc'), ['']),
    multiSearchAny(materialize('abc'), ['a', 'a', 'b']),
    multiSearchAny(materialize('xyz'), ['a', 'a', 'b']),
    multiSearchAnyCaseInsensitive(materialize('abc'), ['ABC', 'abc']),
    multiSearchAnyCaseInsensitive(materialize('über'), ['ÜBER']),
    multiSearchAnyCaseInsensitive(materialize('HELLO world'), ['hello']),
    multiSearchAnyCaseInsensitiveUTF8(materialize('über'), ['ÜBER']),
    multiSearchAnyCaseInsensitiveUTF8(materialize('plain i'), ['İ']),
    multiSearchAnyCaseInsensitiveUTF8(materialize('ǆ'), ['ǅ']),
    multiSearchAnyCaseInsensitiveUTF8(materialize('ⅰ'), ['Ⅰ']),
    multiSearchAnyCaseInsensitiveUTF8(materialize(unhex('FF41')), [unhex('FF61')])
SETTINGS force_daachorse_for_multi_search = 1;

WITH arrayMap(i -> concat('needle_', toString(i)), range(255)) AS needles
SELECT
    length(needles),
    multiSearchAny(materialize('needle_254'), needles),
    multiSearchAny(materialize('missing'), needles);

WITH arrayMap(i -> concat('needle_', toString(i)), range(256)) AS needles
SELECT
    length(needles),
    multiSearchAny(materialize('needle_255'), needles),
    multiSearchAnyCaseInsensitive(materialize('NEEDLE_255'), needles),
    multiSearchAnyUTF8(materialize('needle_255'), needles),
    multiSearchAnyCaseInsensitiveUTF8(materialize('NEEDLE_255'), needles),
    multiSearchAny(materialize('missing'), needles);
