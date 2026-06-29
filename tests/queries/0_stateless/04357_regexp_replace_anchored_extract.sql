-- Regression test for the anchored capture-then-truncate fast path of
-- replaceRegexpAll / replaceRegexpOne (REGEXP_REPLACE), e.g. ClickBench Q28's
-- `^https?://(?:www\.)?([^/]+)/.*$` with replacement `\1`: the fast path strips the
-- trailing `.*$` and emits the captured group. RE2 is dotall here (dot_nl=true) so `.*$`
-- spans newlines; the fast path falls back to the full regexp only when the discarded
-- suffix would not match it -- an invalid-UTF-8 byte, or a newline under an explicit
-- non-dotall scope ([^\n], (?-s)). Covers firing, the fallbacks, and shapes that must
-- NOT trigger the optimization (still correct via the default path).

DROP TABLE IF EXISTS t_anchored_extract;
CREATE TABLE t_anchored_extract (id UInt32, s String) ENGINE = Memory;
INSERT INTO t_anchored_extract VALUES
    (1, 'http://www.example.com/path/to/page?q=1'),
    (2, 'https://example.org/a'),
    (3, 'http://example.com'),
    (4, 'http://www.x.com/'),
    (5, 'http://a.com/b/c/d'),
    (6, 'http://x.com:8080/a'),
    (7, 'https://user:pass@h.com/p'),
    (8, 'http://пример.рф/путь/q'),
    (9, 'HTTP://UPPER.COM/x'),
    (10, 'http://only.host.no.slash'),
    (11, 'http://trailing.slash.com/'),
    (12, 'https://www.sub.dom.co.uk/a/b?x=y#z'),
    (13, 'http://1.2.3.4/page'),
    (14, 'http://[::1]/v6'),
    (15, 'http://x/y'),
    (16, 'https://x.com'),
    (17, 'http://a b.com/p'),
    (18, 'ftp://nomatch.com/a'),
    (19, 'hello world'),
    (20, ''),
    (21, 'http://www.www.com/x'),
    (22, 'http://h.com/very/long/path/segment/here?a=1&b=2'),
    (23, 'http://www.x.com/a\nb'),
    (24, 'http://x.com/\nfoo'),
    (25, 'pre\nhttp://x.com/a'),
    (26, 'http://h.com/a\n'),
    (27, 'http://h.com/\n'),
    (28, '\nhttp://h.com/a'),
    (29, 'http://h.com/a\r\nb'),
    (30, 'http://h.com/a\tb'),
    (31, 'http://h.com/a\0b'),
    (32, 'a\nb'),
    (33, 'abc/def'),
    (34, 'abc'),
    (35, 'a/b/c'),
    (36, 'x/'),
    (37, 'noslashhere'),
    (38, 'a/'),
    (39, '/leading'),
    (40, 'a//b'),
    (41, '123-abc'),
    (42, 'abc-1'),
    (43, 'a.b.c'),
    (44, 'ab.c'),
    (45, 'seg/ab1'),
    (46, 'seg/x\ry');

-- Part A: independent oracle. For anchored single-match bare-backref patterns the result
-- must equal if(match(s,P), extractGroups(s,P)[N], s). match()/extractGroups() do NOT use
-- the optimized code path, so this independently validates it. replaceRegexp, match() and
-- extractGroups() all compile RE2 with dot_nl=true, so '.' matches '\n' consistently and
-- newline-bearing inputs are covered too; explicit non-dotall scopes ([^\n], (?-s)) still
-- exercise the newline fallback. Each line prints 0 mismatches.
SELECT 'q28_host' AS pattern, sum(replaceRegexpAll(s, '^https?://(?:www\\.)?([^/]+)/.*$', '\\1') != if(match(s, '^https?://(?:www\\.)?([^/]+)/.*$'), extractGroups(s, '^https?://(?:www\\.)?([^/]+)/.*$')[1], s)) AS mismatches FROM t_anchored_extract;
SELECT 'simple_first_seg' AS pattern, sum(replaceRegexpAll(s, '^([^/]+)/.*$', '\\1') != if(match(s, '^([^/]+)/.*$'), extractGroups(s, '^([^/]+)/.*$')[1], s)) AS mismatches FROM t_anchored_extract;
SELECT 'scheme_group1' AS pattern, sum(replaceRegexpAll(s, '^(https?)://([^/]+)/.*$', '\\1') != if(match(s, '^(https?)://([^/]+)/.*$'), extractGroups(s, '^(https?)://([^/]+)/.*$')[1], s)) AS mismatches FROM t_anchored_extract;
SELECT 'host_group2' AS pattern, sum(replaceRegexpAll(s, '^(https?)://([^/]+)/.*$', '\\2') != if(match(s, '^(https?)://([^/]+)/.*$'), extractGroups(s, '^(https?)://([^/]+)/.*$')[2], s)) AS mismatches FROM t_anchored_extract;
SELECT 'escaped_dot_prefix' AS pattern, sum(replaceRegexpAll(s, '^(a)\\..*$', '\\1') != if(match(s, '^(a)\\..*$'), extractGroups(s, '^(a)\\..*$')[1], s)) AS mismatches FROM t_anchored_extract;
SELECT 'explicit_nonnl_tail' AS pattern, sum(replaceRegexpAll(s, '^([^/]+)/[^\\n]*$', '\\1') != if(match(s, '^([^/]+)/[^\\n]*$'), extractGroups(s, '^([^/]+)/[^\\n]*$')[1], s)) AS mismatches FROM t_anchored_extract;
SELECT 'no_dotall_scope' AS pattern, sum(replaceRegexpAll(s, '(?-s)^([^/]+)/.*$', '\\1') != if(match(s, '(?-s)^([^/]+)/.*$'), extractGroups(s, '(?-s)^([^/]+)/.*$')[1], s)) AS mismatches FROM t_anchored_extract;
SELECT 'backslash_A_z' AS pattern, sum(replaceRegexpAll(s, '\\A([^/]+)/.*\\z', '\\1') != if(match(s, '\\A([^/]+)/.*\\z'), extractGroups(s, '\\A([^/]+)/.*\\z')[1], s)) AS mismatches FROM t_anchored_extract;
SELECT 'dotall_host' AS pattern, sum(replaceRegexpAll(s, '(?s)^([^/]+)/.*$', '\\1') != if(match(s, '(?s)^([^/]+)/.*$'), extractGroups(s, '(?s)^([^/]+)/.*$')[1], s)) AS mismatches FROM t_anchored_extract;
SELECT 'anchored_no_dotstar' AS pattern, sum(replaceRegexpAll(s, '^(a)bc$', '\\1') != if(match(s, '^(a)bc$'), extractGroups(s, '^(a)bc$')[1], s)) AS mismatches FROM t_anchored_extract;
SELECT 'astar_tail' AS pattern, sum(replaceRegexpAll(s, '^https?://([^/]+)/a*$', '\\1') != if(match(s, '^https?://([^/]+)/a*$'), extractGroups(s, '^https?://([^/]+)/a*$')[1], s)) AS mismatches FROM t_anchored_extract;
SELECT 'greedy_last_slash' AS pattern, sum(replaceRegexpAll(s, '^(.*)/.*$', '\\1') != if(match(s, '^(.*)/.*$'), extractGroups(s, '^(.*)/.*$')[1], s)) AS mismatches FROM t_anchored_extract;
SELECT 'lazy_first_slash' AS pattern, sum(replaceRegexpAll(s, '^(.*?)/.*$', '\\1') != if(match(s, '^(.*?)/.*$'), extractGroups(s, '^(.*?)/.*$')[1], s)) AS mismatches FROM t_anchored_extract;
SELECT 'digits_prefix' AS pattern, sum(replaceRegexpAll(s, '^([0-9]+)-.*$', '\\1') != if(match(s, '^([0-9]+)-.*$'), extractGroups(s, '^([0-9]+)-.*$')[1], s)) AS mismatches FROM t_anchored_extract;
SELECT 'tail_class_has_nl' AS pattern, sum(replaceRegexpAll(s, '^([^/]+)/[^/]*$', '\\1') != if(match(s, '^([^/]+)/[^/]*$'), extractGroups(s, '^([^/]+)/[^/]*$')[1], s)) AS mismatches FROM t_anchored_extract;
SELECT 'tail_class_alpha' AS pattern, sum(replaceRegexpAll(s, '^([^/]+)/[a-z]*$', '\\1') != if(match(s, '^([^/]+)/[a-z]*$'), extractGroups(s, '^([^/]+)/[a-z]*$')[1], s)) AS mismatches FROM t_anchored_extract;
SELECT 'tail_class_no_crlf' AS pattern, sum(replaceRegexpAll(s, '^([^/]+)/[^\\r\\n]*$', '\\1') != if(match(s, '^([^/]+)/[^\\r\\n]*$'), extractGroups(s, '^([^/]+)/[^\\r\\n]*$')[1], s)) AS mismatches FROM t_anchored_extract;

-- Part B: concrete golden behaviour (includes newline fallback rows and non-firing shapes).
-- Output columns: label, id, s, replaceRegexpAll, replaceRegexpOne.
SELECT 'FIRE q28 \\1' AS t, id, s, replaceRegexpAll(s, '^https?://(?:www\\.)?([^/]+)/.*$', '\\1') AS all_, replaceRegexpOne(s, '^https?://(?:www\\.)?([^/]+)/.*$', '\\1') AS one_ FROM t_anchored_extract ORDER BY id;
SELECT 'FIRE scheme \\1' AS t, id, s, replaceRegexpAll(s, '^(https?)://([^/]+)/.*$', '\\1') AS all_, replaceRegexpOne(s, '^(https?)://([^/]+)/.*$', '\\1') AS one_ FROM t_anchored_extract ORDER BY id;
SELECT 'FIRE host \\2' AS t, id, s, replaceRegexpAll(s, '^(https?)://([^/]+)/.*$', '\\2') AS all_, replaceRegexpOne(s, '^(https?)://([^/]+)/.*$', '\\2') AS one_ FROM t_anchored_extract ORDER BY id;
SELECT 'FIRE dotall \\1' AS t, id, s, replaceRegexpAll(s, '(?s)^([^/]+)/.*$', '\\1') AS all_, replaceRegexpOne(s, '(?s)^([^/]+)/.*$', '\\1') AS one_ FROM t_anchored_extract ORDER BY id;
SELECT 'FIRE explicit[^nl] \\1' AS t, id, s, replaceRegexpAll(s, '^([^/]+)/[^\\n]*$', '\\1') AS all_, replaceRegexpOne(s, '^([^/]+)/[^\\n]*$', '\\1') AS one_ FROM t_anchored_extract ORDER BY id;
SELECT 'NOFIRE repl x\\1' AS t, id, s, replaceRegexpAll(s, '^https?://(?:www\\.)?([^/]+)/.*$', 'x\\1') AS all_, replaceRegexpOne(s, '^https?://(?:www\\.)?([^/]+)/.*$', 'x\\1') AS one_ FROM t_anchored_extract ORDER BY id;
SELECT 'NOFIRE repl \\2-\\1' AS t, id, s, replaceRegexpAll(s, '^(https?)://([^/]+)/.*$', '\\2-\\1') AS all_, replaceRegexpOne(s, '^(https?)://([^/]+)/.*$', '\\2-\\1') AS one_ FROM t_anchored_extract ORDER BY id;
SELECT 'NOFIRE astar_tail \\1' AS t, id, s, replaceRegexpAll(s, '^https?://([^/]+)/a*$', '\\1') AS all_, replaceRegexpOne(s, '^https?://([^/]+)/a*$', '\\1') AS one_ FROM t_anchored_extract ORDER BY id;
SELECT 'NOFIRE no_dotstar \\1' AS t, id, s, replaceRegexpAll(s, '^(a)bc$', '\\1') AS all_, replaceRegexpOne(s, '^(a)bc$', '\\1') AS one_ FROM t_anchored_extract ORDER BY id;
SELECT 'NOFIRE no_begin \\1' AS t, id, s, replaceRegexpAll(s, '(foo).*$', '\\1') AS all_, replaceRegexpOne(s, '(foo).*$', '\\1') AS one_ FROM t_anchored_extract ORDER BY id;
SELECT 'NOFIRE no_end \\1' AS t, id, s, replaceRegexpAll(s, '^(foo).*', '\\1') AS all_, replaceRegexpOne(s, '^(foo).*', '\\1') AS one_ FROM t_anchored_extract ORDER BY id;
SELECT 'NOFIRE multiline \\1' AS t, id, s, replaceRegexpAll(s, '(?m)^(\\w+)$', '\\1') AS all_, replaceRegexpOne(s, '(?m)^(\\w+)$', '\\1') AS one_ FROM t_anchored_extract ORDER BY id;
SELECT 'NOFIRE group_is_dotstar' AS t, id, s, replaceRegexpAll(s, '^(.*)$', '\\1') AS all_, replaceRegexpOne(s, '^(.*)$', '\\1') AS one_ FROM t_anchored_extract ORDER BY id;
SELECT 'NOFIRE tail[^/]* \\1' AS t, id, s, replaceRegexpAll(s, '^([^/]+)/[^/]*$', '\\1') AS all_, replaceRegexpOne(s, '^([^/]+)/[^/]*$', '\\1') AS one_ FROM t_anchored_extract ORDER BY id;

DROP TABLE t_anchored_extract;

-- Part C: FixedString haystack (fixed width, '\0'-padded). '\0' is an ordinary byte to the
-- regexp, so results match a String haystack: a match yields the clean capture, a non-match
-- returns the whole padded value verbatim. hex() keeps the padding visible and the reference
-- plain text. Columns: label, id, hex(input), hex(replaceRegexpAll), hex(replaceRegexpOne).
DROP TABLE IF EXISTS t_anchored_extract_fixed;
CREATE TABLE t_anchored_extract_fixed (id UInt32, s FixedString(40)) ENGINE = Memory;
INSERT INTO t_anchored_extract_fixed VALUES
    (1, 'http://www.example.com/p'),
    (2, 'http://h.com'),
    (3, 'http://h.com/a\nb'),
    (4, 'http://x.com:8080/path'),
    (5, ''),
    (6, 'http://пример.рф/q'),
    (7, 'http://h.com/'),
    (8, 'http://h/a\0b'),
    (9, 'no-match'),
    (10, 'a/b/c');
SELECT 'FS q28 \\1' AS t, id, hex(s) AS s_hex, hex(replaceRegexpAll(s, '^https?://(?:www\\.)?([^/]+)/.*$', '\\1')) AS all_hex, hex(replaceRegexpOne(s, '^https?://(?:www\\.)?([^/]+)/.*$', '\\1')) AS one_hex FROM t_anchored_extract_fixed ORDER BY id;
SELECT 'FS host \\2' AS t, id, hex(s) AS s_hex, hex(replaceRegexpAll(s, '^(https?)://([^/]+)/.*$', '\\2')) AS all_hex, hex(replaceRegexpOne(s, '^(https?)://([^/]+)/.*$', '\\2')) AS one_hex FROM t_anchored_extract_fixed ORDER BY id;
SELECT 'FS dotall \\1' AS t, id, hex(s) AS s_hex, hex(replaceRegexpAll(s, '(?s)^([^/]+)/.*$', '\\1')) AS all_hex, hex(replaceRegexpOne(s, '(?s)^([^/]+)/.*$', '\\1')) AS one_hex FROM t_anchored_extract_fixed ORDER BY id;
SELECT 'FS noanchor \\1' AS t, id, hex(s) AS s_hex, hex(replaceRegexpAll(s, '(foo).*$', '\\1')) AS all_hex, hex(replaceRegexpOne(s, '(foo).*$', '\\1')) AS one_hex FROM t_anchored_extract_fixed ORDER BY id;
DROP TABLE t_anchored_extract_fixed;

-- Part D: a scoped multiline anchor `(?m:…)` in the retained prefix must reject the fast path and fall back
-- to the full regexp. re2::Regexp::ToString renders `(?m:^)`/`(?m:$)` as bare `^`/`$`, which would reparse
-- as text anchors under RE2's default one-line mode and corrupt the shortened regexp. These patterns are
-- dotall (`(?s)`), so replaceRegexp and match()/extractGroups() agree on `.`, which lets the oracle below
-- cover newline-bearing rows too (unlike Part A). Covers the line anchor at top level and nested in a group.
DROP TABLE IF EXISTS t_anchored_extract_ml;
CREATE TABLE t_anchored_extract_ml (id UInt32, s String) ENGINE = Memory;
INSERT INTO t_anchored_extract_ml VALUES
    (1, 'a\nb'), (2, 'a\nbZZ'), (3, 'x\na\nb'), (4, 'qqq'), (5, 'a\nb\nc');

-- Oracle: the optimized result must equal if(match(s,P), extractGroups(s,P)[N], s). Each line prints 0.
SELECT 'ml_end \\1' AS pattern, sum(replaceRegexpAll(s, '(?s)^(a)(?m:$)\\nb.*$', '\\1') != if(match(s, '(?s)^(a)(?m:$)\\nb.*$'), extractGroups(s, '(?s)^(a)(?m:$)\\nb.*$')[1], s)) AS mismatches FROM t_anchored_extract_ml;
SELECT 'ml_begin \\1' AS pattern, sum(replaceRegexpAll(s, '(?s)^(a)\\n(?m:^)b.*$', '\\1') != if(match(s, '(?s)^(a)\\n(?m:^)b.*$'), extractGroups(s, '(?s)^(a)\\n(?m:^)b.*$')[1], s)) AS mismatches FROM t_anchored_extract_ml;
SELECT 'ml_nested \\1' AS pattern, sum(replaceRegexpAll(s, '(?s)^(a(?m:$))\\nb.*$', '\\1') != if(match(s, '(?s)^(a(?m:$))\\nb.*$'), extractGroups(s, '(?s)^(a(?m:$))\\nb.*$')[1], s)) AS mismatches FROM t_anchored_extract_ml;

-- Golden: concrete optimized output for the top-level case (hex keeps the newlines readable).
SELECT 'ml_end \\1' AS t, id, hex(s) AS s_hex, hex(replaceRegexpAll(s, '(?s)^(a)(?m:$)\\nb.*$', '\\1')) AS all_hex, hex(replaceRegexpOne(s, '(?s)^(a)(?m:$)\\nb.*$', '\\1')) AS one_hex FROM t_anchored_extract_ml ORDER BY id;
DROP TABLE t_anchored_extract_ml;

-- Part E: an invalid-UTF-8 byte in the discarded suffix must also reject the fast path. RE2 runs in UTF-8
-- mode, so the original `.*$` only matches a suffix that is valid UTF-8; without this guard the short
-- pattern would emit the capture for inputs where the full regexp does not match. Covers a lone invalid
-- byte, a broken multibyte sequence, a surrogate encoding, plus valid ASCII / multibyte controls.
DROP TABLE IF EXISTS t_anchored_extract_utf8;
CREATE TABLE t_anchored_extract_utf8 (id UInt32, s String) ENGINE = Memory;
INSERT INTO t_anchored_extract_utf8 VALUES
    (1, concat('a/', unhex('FF'))),
    (2, 'a/b'),
    (3, concat('a/', unhex('C328'))),
    (4, concat('a/', unhex('D18F'))),
    (5, concat('a/', unhex('EDA080'))),
    (6, 'zzz');

-- Oracle (inputs are newline-free, so non-dotall and dotall agree on `.`): the optimized result must equal
-- if(match(s,P), extractGroups(s,P)[N], s). Each line prints 0.
SELECT 'utf8 non-dotall \\1' AS pattern, sum(replaceRegexpAll(s, '^(a)/.*$', '\\1') != if(match(s, '^(a)/.*$'), extractGroups(s, '^(a)/.*$')[1], s)) AS mismatches FROM t_anchored_extract_utf8;
SELECT 'utf8 dotall \\1' AS pattern, sum(replaceRegexpAll(s, '(?s)^(a)/.*$', '\\1') != if(match(s, '(?s)^(a)/.*$'), extractGroups(s, '(?s)^(a)/.*$')[1], s)) AS mismatches FROM t_anchored_extract_utf8;

-- Golden: concrete optimized output (hex keeps the raw bytes readable). Columns: label, id, hex(s), all, one.
SELECT 'utf8 non-dotall \\1' AS t, id, hex(s) AS s_hex, hex(replaceRegexpAll(s, '^(a)/.*$', '\\1')) AS all_hex, hex(replaceRegexpOne(s, '^(a)/.*$', '\\1')) AS one_hex FROM t_anchored_extract_utf8 ORDER BY id;
DROP TABLE t_anchored_extract_utf8;
